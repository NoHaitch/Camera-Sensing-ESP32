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
static const int N_SEND    = 10;  // experimen: 10/20/50

#define MAX_SAMPLES 100

static int g_meta_msg_id[MAX_SAMPLES];
static int g_raw_msg_id[MAX_SAMPLES];

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

static void stats_int64_to_ms(const int64_t *arr, const bool *valid, int n,
                              double *min_ms, double *max_ms, double *avg_ms, int *count_v)
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
        *min_ms = 0.0;
        *max_ms = 0.0;
        *avg_ms = 0.0;
    } else {
        *min_ms = min_val / 1000.0;
        *max_ms = max_val / 1000.0;
        *avg_ms = (sum / count) / 1000.0;
    }
}

static void print_stats_block_ms(const char *title, const int64_t *arr, const bool *valid, int n)
{
    double min_ms, max_ms, avg_ms;
    int count_v;

    stats_int64_to_ms(arr, valid, n, &min_ms, &max_ms, &avg_ms, &count_v);

    ESP_LOGI(TAG, "%s", title);
    ESP_LOGI(TAG, "  count  : %d", count_v);

    if (count_v == 0) {
        ESP_LOGI(TAG, "  min_ms : N/A");
        ESP_LOGI(TAG, "  max_ms : N/A");
        ESP_LOGI(TAG, "  avg_ms : N/A");
        return;
    }

    ESP_LOGI(TAG, "  min_ms : %.3f", min_ms);
    ESP_LOGI(TAG, "  max_ms : %.3f", max_ms);
    ESP_LOGI(TAG, "  avg_ms : %.3f", avg_ms);
}

static void print_array_ms(const char *title, const int64_t *arr, const bool *valid, int n)
{
    char line[2048];
    int offset = 0;

    offset += snprintf(line + offset, sizeof(line) - offset, "%s [", title);

    bool first = true;
    for (int i = 0; i < n; i++) {
        if (valid != NULL && !valid[i]) {
            continue;
        }

        double value_ms = arr[i] / 1000.0;

        if (!first) {
            offset += snprintf(line + offset, sizeof(line) - offset, ", ");
        }

        offset += snprintf(line + offset, sizeof(line) - offset, "%.3f", value_ms);
        first = false;

        if (offset >= (int)sizeof(line) - 32) {
            break;
        }
    }

    snprintf(line + offset, sizeof(line) - offset, "]");
    ESP_LOGI(TAG, "%s", line);
}

static void print_experiment_summary(size_t image_size_bytes)
{
    print_separator();
    ESP_LOGI(TAG, "FINAL SUMMARY");
    print_separator();

    ESP_LOGI(TAG, "topic            : %s", CONFIG_APP_MQTT_TOPIC);
    ESP_LOGI(TAG, "target_interval  : %d s", T_SECONDS);
    ESP_LOGI(TAG, "send_count       : %d", N_SEND);
    ESP_LOGI(TAG, "image_size_bytes : %u", (unsigned int)image_size_bytes);

    print_separator();
    ESP_LOGI(TAG, "RAW DATA ARRAYS (ms)");
    print_array_ms("meta_call_ms", g_meta_call_duration_us, NULL,                N_SEND);
    print_array_ms("meta_ack_ms",  g_meta_ack_latency_us,   g_meta_ack_received, N_SEND);
    print_array_ms("raw_call_ms",  g_raw_call_duration_us,  NULL,                N_SEND);
    print_array_ms("raw_ack_ms",   g_raw_ack_latency_us,    g_raw_ack_received,  N_SEND);

    print_separator();
    ESP_LOGI(TAG, "META");
    print_stats_block_ms("call_duration", g_meta_call_duration_us, NULL,                N_SEND);
    print_stats_block_ms("ack_latency",   g_meta_ack_latency_us,   g_meta_ack_received, N_SEND);

    print_separator();
    ESP_LOGI(TAG, "RAW");
    print_stats_block_ms("call_duration", g_raw_call_duration_us, NULL,               N_SEND);
    print_stats_block_ms("ack_latency",   g_raw_ack_latency_us,   g_raw_ack_received, N_SEND);

    print_separator();
}

static void publish_image_once(esp_mqtt_client_handle_t client, int seq)
{
    const uint8_t *image_data = test_image_jpg_start;
    size_t image_size_bytes = test_image_jpg_end - test_image_jpg_start;

    char meta_topic[128];
    char raw_topic[128];
    build_topic(meta_topic, sizeof(meta_topic), "meta");
    build_topic(raw_topic, sizeof(raw_topic), "raw");

    int idx = seq - 1;
    int64_t ts_send_us = esp_timer_get_time();

    char meta_payload[256];
    snprintf(meta_payload, sizeof(meta_payload),
             "{\"seq\":%d,\"timestamp_us\":%" PRId64 ",\"filename\":\"test-image.jpg\",\"size_bytes\":%u}",
             seq, ts_send_us, (unsigned int)image_size_bytes);

    ESP_LOGI(TAG, "SEND #%d / %d", seq, N_SEND);

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
}

static void image_publish_task(void *pvParameters)
{
    size_t image_size_bytes = test_image_jpg_end - test_image_jpg_start;

    memset(g_meta_msg_id, 0, sizeof(g_meta_msg_id));
    memset(g_raw_msg_id, 0, sizeof(g_raw_msg_id));
    memset(g_meta_publish_start_us, 0, sizeof(g_meta_publish_start_us));
    memset(g_meta_call_duration_us, 0, sizeof(g_meta_call_duration_us));
    memset(g_meta_ack_latency_us, 0, sizeof(g_meta_ack_latency_us));
    memset(g_raw_publish_start_us, 0, sizeof(g_raw_publish_start_us));
    memset(g_raw_call_duration_us, 0, sizeof(g_raw_call_duration_us));
    memset(g_raw_ack_latency_us, 0, sizeof(g_raw_ack_latency_us));
    memset(g_meta_ack_received, 0, sizeof(g_meta_ack_received));
    memset(g_raw_ack_received, 0, sizeof(g_raw_ack_received));

    for (int seq = 1; seq <= N_SEND; seq++) {
        if (!g_mqtt_connected) {
            break;
        }

        publish_image_once(g_client, seq);

        if (seq < N_SEND) {
            vTaskDelay(pdMS_TO_TICKS(T_SECONDS * 1000));
        }
    }

    vTaskDelay(pdMS_TO_TICKS(2000));

    print_experiment_summary(image_size_bytes);

    g_publish_task_started = false;
    vTaskDelete(NULL);
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    esp_mqtt_client_handle_t client = event->client;

    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED:
            g_client = client;
            g_mqtt_connected = true;

            if (!g_publish_task_started) {
                g_publish_task_started = true;
                xTaskCreate(image_publish_task, "image_publish_task", 12288, NULL, 5, NULL);
            }
            break;

        case MQTT_EVENT_DISCONNECTED:
            g_mqtt_connected = false;
            break;

        case MQTT_EVENT_PUBLISHED: {
            int64_t now_us = esp_timer_get_time();

            for (int i = 0; i < N_SEND; i++) {
                if (event->msg_id == g_meta_msg_id[i]) {
                    g_meta_ack_latency_us[i] = now_us - g_meta_publish_start_us[i];
                    g_meta_ack_received[i] = true;
                    break;
                }

                if (event->msg_id == g_raw_msg_id[i]) {
                    g_raw_ack_latency_us[i] = now_us - g_raw_publish_start_us[i];
                    g_raw_ack_received[i] = true;
                    break;
                }
            }
            break;
        }

        case MQTT_EVENT_ERROR:
            if (event->error_handle &&
                event->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT) {
                ESP_LOGE(TAG, "esp_tls_error_code : 0x%x", event->error_handle->esp_tls_last_esp_err);
                ESP_LOGE(TAG, "tls_stack_error_code : 0x%x", event->error_handle->esp_tls_stack_err);
                ESP_LOGE(TAG, "socket_errno : %d (%s)",
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
