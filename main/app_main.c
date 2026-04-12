#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "nvs_flash.h"
#include "sdkconfig.h"

#include "protocol_examples_common.h"

#include "esp_crt_bundle.h"
#include "mqtt_client.h"

static const char *TAG = "mqtt_image";

extern const uint8_t test_image_jpg_start[] asm("_binary_test_image_jpg_start");
extern const uint8_t test_image_jpg_end[]   asm("_binary_test_image_jpg_end");

static esp_mqtt_client_handle_t g_client = NULL;
static bool g_mqtt_connected = false;
static bool g_publish_task_started = false;

static const int T_SECONDS = 2;   // NIM 13522091 -> mod(91,10)+1 = 2
static const int N_SEND    = 10;  // experiment: 10/20/50 

#define MAX_SAMPLES 100

static int g_meta_msg_id[MAX_SAMPLES];
static int g_raw_msg_id[MAX_SAMPLES];

static int64_t g_send_start_us[MAX_SAMPLES];
static int64_t g_send_interval_us[MAX_SAMPLES];

static int64_t g_meta_publish_start_us[MAX_SAMPLES];
static int64_t g_meta_call_duration_us[MAX_SAMPLES];
static int64_t g_meta_ack_latency_us[MAX_SAMPLES];

static int64_t g_raw_publish_start_us[MAX_SAMPLES];
static int64_t g_raw_call_duration_us[MAX_SAMPLES];
static int64_t g_raw_ack_latency_us[MAX_SAMPLES];

static bool g_meta_ack_received[MAX_SAMPLES];
static bool g_raw_ack_received[MAX_SAMPLES];

static void print_separator(void) 
{
    ESP_LOGI(TAG, "============================================================");
}

static void build_topic(char *buffer, size_t buffer_size, const char *suffix)
{
    snprintf(buffer, buffer_size, "%s/%s", CONFIG_APP_MQTT_TOPIC, suffix);
}

static void stats_int64(const int64_t *arr, const bool *valid, int n,
                        int64_t *min_v, int64_t *max_v, double *avg_v, int *count_v)
{
    int count = 0;
    int64_t min_val = 0;
    int64_t max_val = 0;
    double sum = 0.0;

    for (int i = 0; i < n; i++) {
        if (valid != NULL && !valid[i]) {
            continue;
        }

        int64_t v = arr[i];
        if (count == 0) {
            min_val = v;
            max_val = v;
        } else {
            if (v < min_val) min_val = v;
            if (v > max_val) max_val = v;
        }

        sum += (double)v;
        count++;
    }

    *count_v = count;
    if (count == 0) {
        *min_v = 0;
        *max_v = 0;
        *avg_v = 0.0;
    } else {
        *min_v = min_val;
        *max_v = max_val;
        *avg_v = sum / count;
    }
}

static void print_stats_block(const char *title, const int64_t *arr, const bool *valid, int n)
{
    int64_t min_v, max_v;
    double avg_v;
    int count_v;

    stats_int64(arr, valid, n, &min_v, &max_v, &avg_v, &count_v);

    ESP_LOGI(TAG, "%s", title);
    ESP_LOGI(TAG, "  sample_count        : %d", count_v);

    if (count_v == 0) {
        ESP_LOGI(TAG, "  min                 : N/A");
        ESP_LOGI(TAG, "  max                 : N/A");
        ESP_LOGI(TAG, "  avg                 : N/A");
        return;
    }

    ESP_LOGI(TAG, "  min_us              : %" PRId64 " us", min_v);
    ESP_LOGI(TAG, "  min_ms              : %.3f ms", min_v / 1000.0);
    ESP_LOGI(TAG, "  max_us              : %" PRId64 " us", max_v);
    ESP_LOGI(TAG, "  max_ms              : %.3f ms", max_v / 1000.0);
    ESP_LOGI(TAG, "  avg_us              : %.3f us", avg_v);
    ESP_LOGI(TAG, "  avg_ms              : %.3f ms", avg_v / 1000.0);
}

static void print_experiment_summary(size_t image_size_bytes)
{
    bool interval_valid[MAX_SAMPLES] = {0};

    for (int i = 0; i < N_SEND; i++) {
        interval_valid[i] = (i > 0);
    }

    print_separator();
    ESP_LOGI(TAG, "FINAL EXPERIMENT SUMMARY");
    print_separator();

    ESP_LOGI(TAG, "CONFIGURATION");
    ESP_LOGI(TAG, "  base_topic          : %s", CONFIG_APP_MQTT_TOPIC);
    ESP_LOGI(TAG, "  interval_target_s   : %d s", T_SECONDS);
    ESP_LOGI(TAG, "  total_send_count    : %d", N_SEND);
    ESP_LOGI(TAG, "  image_size_bytes    : %u bytes", (unsigned int)image_size_bytes);

    print_separator();
    print_stats_block("INTERVAL SUMMARY (actual send start interval)", g_send_interval_us, interval_valid, N_SEND);

    print_separator();
    ESP_LOGI(TAG, "META SUMMARY");
    print_stats_block("META publish call duration", g_meta_call_duration_us, NULL, N_SEND);
    print_stats_block("META broker ack latency", g_meta_ack_latency_us, g_meta_ack_received, N_SEND);

    print_separator();
    ESP_LOGI(TAG, "RAW SUMMARY");
    print_stats_block("RAW publish call duration", g_raw_call_duration_us, NULL, N_SEND);
    print_stats_block("RAW broker ack latency", g_raw_ack_latency_us, g_raw_ack_received, N_SEND);

    print_separator();
    ESP_LOGI(TAG, "NOTE");
    ESP_LOGI(TAG, "  - Interval summary di atas adalah interval antar awal pengiriman di sisi ESP32.");
    ESP_LOGI(TAG, "  - Ack latency adalah waktu sampai broker mengirim MQTT_EVENT_PUBLISHED.");
    ESP_LOGI(TAG, "  - End-to-end latency resmi tugas tetap harus dihitung di sisi subscriber/user.");
    print_separator();
}

static void publish_image_once(esp_mqtt_client_handle_t client, int seq, int64_t prev_start_us)
{
    const uint8_t *image_data = test_image_jpg_start;
    size_t image_size_bytes = test_image_jpg_end - test_image_jpg_start;

    char meta_topic[128];
    char raw_topic[128];
    build_topic(meta_topic, sizeof(meta_topic), "meta");
    build_topic(raw_topic, sizeof(raw_topic), "raw");

    int idx = seq - 1;

    int64_t ts_send_us = esp_timer_get_time();
    g_send_start_us[idx] = ts_send_us;
    g_send_interval_us[idx] = (idx == 0) ? 0 : (ts_send_us - prev_start_us);

    char meta_payload[256];
    snprintf(meta_payload, sizeof(meta_payload),
             "{\"seq\":%d,\"timestamp_us\":%" PRId64 ",\"filename\":\"test-image.jpg\",\"size_bytes\":%u}",
             seq, ts_send_us, (unsigned int)image_size_bytes);

    print_separator();
    ESP_LOGI(TAG, "SEND #%d / %d", seq, N_SEND);
    ESP_LOGI(TAG, "target_interval_s        : %d s", T_SECONDS);
    ESP_LOGI(TAG, "timestamp_send_us        : %" PRId64 " us", ts_send_us);
    ESP_LOGI(TAG, "timestamp_send_ms        : %.3f ms", ts_send_us / 1000.0);

    if (idx == 0) {
        ESP_LOGI(TAG, "actual_interval          : N/A (first send)");
    } else {
        ESP_LOGI(TAG, "actual_interval_us       : %" PRId64 " us", g_send_interval_us[idx]);
        ESP_LOGI(TAG, "actual_interval_ms       : %.3f ms", g_send_interval_us[idx] / 1000.0);
        ESP_LOGI(TAG, "actual_interval_s        : %.3f s", g_send_interval_us[idx] / 1000000.0);
    }

    ESP_LOGI(TAG, "image_size_bytes         : %u bytes", (unsigned int)image_size_bytes);
    ESP_LOGI(TAG, "meta_topic               : %s", meta_topic);
    ESP_LOGI(TAG, "raw_topic                : %s", raw_topic);

    g_meta_ack_received[idx] = false;
    g_raw_ack_received[idx] = false;

    g_meta_publish_start_us[idx] = esp_timer_get_time();
    g_meta_msg_id[idx] = esp_mqtt_client_publish(
        client,
        meta_topic,
        meta_payload,
        0,
        1,
        0
    );
    int64_t meta_publish_end_us = esp_timer_get_time();
    g_meta_call_duration_us[idx] = meta_publish_end_us - g_meta_publish_start_us[idx];

    ESP_LOGI(TAG, "META publish()");
    ESP_LOGI(TAG, "meta_msg_id              : %d", g_meta_msg_id[idx]);
    ESP_LOGI(TAG, "meta_payload_size_bytes  : %u bytes", (unsigned int)strlen(meta_payload));
    ESP_LOGI(TAG, "meta_call_duration_us    : %" PRId64 " us", g_meta_call_duration_us[idx]);
    ESP_LOGI(TAG, "meta_call_duration_ms    : %.3f ms", g_meta_call_duration_us[idx] / 1000.0);

    g_raw_publish_start_us[idx] = esp_timer_get_time();
    g_raw_msg_id[idx] = esp_mqtt_client_publish(
        client,
        raw_topic,
        (const char *)image_data,
        (int)image_size_bytes,
        1,
        0
    );
    int64_t raw_publish_end_us = esp_timer_get_time();
    g_raw_call_duration_us[idx] = raw_publish_end_us - g_raw_publish_start_us[idx];

    ESP_LOGI(TAG, "RAW IMAGE publish()");
    ESP_LOGI(TAG, "raw_msg_id               : %d", g_raw_msg_id[idx]);
    ESP_LOGI(TAG, "raw_payload_size_bytes   : %u bytes", (unsigned int)image_size_bytes);
    ESP_LOGI(TAG, "raw_call_duration_us     : %" PRId64 " us", g_raw_call_duration_us[idx]);
    ESP_LOGI(TAG, "raw_call_duration_ms     : %.3f ms", g_raw_call_duration_us[idx] / 1000.0);
    ESP_LOGI(TAG, "total_send_call_us       : %" PRId64 " us", raw_publish_end_us - ts_send_us);
    ESP_LOGI(TAG, "total_send_call_ms       : %.3f ms", (raw_publish_end_us - ts_send_us) / 1000.0);
    print_separator();
}

static void image_publish_task(void *pvParameters)
{
    int64_t prev_start_us = 0;
    size_t image_size_bytes = test_image_jpg_end - test_image_jpg_start;

    memset(g_meta_msg_id, 0, sizeof(g_meta_msg_id));
    memset(g_raw_msg_id, 0, sizeof(g_raw_msg_id));
    memset(g_send_start_us, 0, sizeof(g_send_start_us));
    memset(g_send_interval_us, 0, sizeof(g_send_interval_us));
    memset(g_meta_publish_start_us, 0, sizeof(g_meta_publish_start_us));
    memset(g_meta_call_duration_us, 0, sizeof(g_meta_call_duration_us));
    memset(g_meta_ack_latency_us, 0, sizeof(g_meta_ack_latency_us));
    memset(g_raw_publish_start_us, 0, sizeof(g_raw_publish_start_us));
    memset(g_raw_call_duration_us, 0, sizeof(g_raw_call_duration_us));
    memset(g_raw_ack_latency_us, 0, sizeof(g_raw_ack_latency_us));
    memset(g_meta_ack_received, 0, sizeof(g_meta_ack_received));
    memset(g_raw_ack_received, 0, sizeof(g_raw_ack_received));

    ESP_LOGI(TAG, "Publisher task started");
    ESP_LOGI(TAG, "configured_base_topic    : %s", CONFIG_APP_MQTT_TOPIC);
    ESP_LOGI(TAG, "configured_interval_s    : %d s", T_SECONDS);
    ESP_LOGI(TAG, "configured_send_count    : %d", N_SEND);

    for (int seq = 1; seq <= N_SEND; seq++) {
        if (!g_mqtt_connected) {
            ESP_LOGW(TAG, "MQTT disconnected, stopping publisher task");
            break;
        }

        int64_t loop_start_us = esp_timer_get_time();
        publish_image_once(g_client, seq, prev_start_us);
        prev_start_us = loop_start_us;

        if (seq < N_SEND) {
            ESP_LOGI(TAG, "wait_before_next_send   : %d s", T_SECONDS);
            vTaskDelay(pdMS_TO_TICKS(T_SECONDS * 1000));
        }
    }

    vTaskDelay(pdMS_TO_TICKS(2000));

    print_experiment_summary(image_size_bytes);

    ESP_LOGI(TAG, "Publisher task finished");
    g_publish_task_started = false;
    vTaskDelete(NULL);
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    esp_mqtt_client_handle_t client = event->client;

    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED:
            ESP_LOGI(TAG, "MQTT connected");
            g_client = client;
            g_mqtt_connected = true;

            if (!g_publish_task_started) {
                g_publish_task_started = true;
                xTaskCreate(image_publish_task, "image_publish_task", 12288, NULL, 5, NULL);
            }
            break;

        case MQTT_EVENT_DISCONNECTED:
            ESP_LOGW(TAG, "MQTT disconnected");
            g_mqtt_connected = false;
            break;

        case MQTT_EVENT_PUBLISHED: {
            int64_t now_us = esp_timer_get_time();
            bool found = false;

            for (int i = 0; i < N_SEND; i++) {
                if (event->msg_id == g_meta_msg_id[i]) {
                    g_meta_ack_latency_us[i] = now_us - g_meta_publish_start_us[i];
                    g_meta_ack_received[i] = true;

                    print_separator();
                    ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED");
                    ESP_LOGI(TAG, "ack_type                : metadata");
                    ESP_LOGI(TAG, "send_index              : %d", i + 1);
                    ESP_LOGI(TAG, "ack_msg_id              : %d", event->msg_id);
                    ESP_LOGI(TAG, "ack_latency_us          : %" PRId64 " us", g_meta_ack_latency_us[i]);
                    ESP_LOGI(TAG, "ack_latency_ms          : %.3f ms", g_meta_ack_latency_us[i] / 1000.0);
                    print_separator();

                    found = true;
                    break;
                }

                if (event->msg_id == g_raw_msg_id[i]) {
                    g_raw_ack_latency_us[i] = now_us - g_raw_publish_start_us[i];
                    g_raw_ack_received[i] = true;

                    print_separator();
                    ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED");
                    ESP_LOGI(TAG, "ack_type                : raw_image");
                    ESP_LOGI(TAG, "send_index              : %d", i + 1);
                    ESP_LOGI(TAG, "ack_msg_id              : %d", event->msg_id);
                    ESP_LOGI(TAG, "ack_latency_us          : %" PRId64 " us", g_raw_ack_latency_us[i]);
                    ESP_LOGI(TAG, "ack_latency_ms          : %.3f ms", g_raw_ack_latency_us[i] / 1000.0);
                    print_separator();

                    found = true;
                    break;
                }
            }

            if (!found) {
                ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED unknown msg_id=%d", event->msg_id);
            }
            break;
        }

        case MQTT_EVENT_ERROR:
            ESP_LOGE(TAG, "MQTT error");
            if (event->error_handle &&
                event->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT) {
                ESP_LOGE(TAG, "esp_tls_error_code      : 0x%x", event->error_handle->esp_tls_last_esp_err);
                ESP_LOGE(TAG, "tls_stack_error_code    : 0x%x", event->error_handle->esp_tls_stack_err);
                ESP_LOGE(TAG, "socket_errno            : %d (%s)",
                         event->error_handle->esp_transport_sock_errno,
                         strerror(event->error_handle->esp_transport_sock_errno));
            }
            break;

        default:
            break;
    }
}

static void mqtt_app_start(void)
{
    const esp_mqtt_client_config_t mqtt_cfg = {
        .broker = {
            .address.uri = CONFIG_APP_MQTT_BROKER_URI,
            .verification.crt_bundle_attach = esp_crt_bundle_attach,
        },
        .credentials = {
            .username = CONFIG_APP_MQTT_USERNAME,
            .authentication.password = CONFIG_APP_MQTT_PASSWORD,
        },
    };

    esp_mqtt_client_handle_t client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(client);
}

void app_main(void)
{
    ESP_LOGI(TAG, "Starting app");
    ESP_LOGI(TAG, "free_heap_bytes         : %" PRIu32 " bytes", esp_get_free_heap_size());
    ESP_LOGI(TAG, "esp_idf_version         : %s", esp_get_idf_version());

    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    ESP_ERROR_CHECK(example_connect());

    mqtt_app_start();
}
