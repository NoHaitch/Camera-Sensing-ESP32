#include <stdio.h>
#include <inttypes.h>
#include <string.h>

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

static void publish_image(esp_mqtt_client_handle_t client)
{
    const uint8_t *image_data = test_image_jpg_start;
    size_t image_size = test_image_jpg_end - test_image_jpg_start;

    int64_t ts_us = esp_timer_get_time();

    char meta_payload[256];
    snprintf(meta_payload, sizeof(meta_payload),
             "{\"timestamp_us\":%" PRId64 ",\"filename\":\"test-image.jpg\",\"size\":%u}",
             ts_us, (unsigned int)image_size);

    int meta_msg_id = esp_mqtt_client_publish(
        client,
        "if4051/13522091/image/meta",
        meta_payload,
        0,
        1,
        0
    );

    ESP_LOGI(TAG, "Published metadata, msg_id=%d", meta_msg_id);
    ESP_LOGI(TAG, "Image size = %u bytes", (unsigned int)image_size);

    int img_msg_id = esp_mqtt_client_publish(
        client,
        "if4051/13522091/image/raw",
        (const char *)image_data,
        (int)image_size,
        1,
        0
    );

    ESP_LOGI(TAG, "Published image, msg_id=%d", img_msg_id);
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    esp_mqtt_client_handle_t client = event->client;

    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED:
            ESP_LOGI(TAG, "MQTT connected");
            publish_image(client);
            break;

        case MQTT_EVENT_DISCONNECTED:
            ESP_LOGI(TAG, "MQTT disconnected");
            break;

        case MQTT_EVENT_PUBLISHED:
            ESP_LOGI(TAG, "Message published, msg_id=%d", event->msg_id);
            break;

        case MQTT_EVENT_ERROR:
            ESP_LOGE(TAG, "MQTT error");
            if (event->error_handle &&
                event->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT) {
                ESP_LOGE(TAG, "esp-tls err=0x%x", event->error_handle->esp_tls_last_esp_err);
                ESP_LOGE(TAG, "tls stack err=0x%x", event->error_handle->esp_tls_stack_err);
                ESP_LOGE(TAG, "sock errno=%d (%s)",
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
    ESP_LOGI(TAG, "Free memory: %" PRIu32 " bytes", esp_get_free_heap_size());
    ESP_LOGI(TAG, "ESP-IDF version: %s", esp_get_idf_version());

    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    ESP_ERROR_CHECK(example_connect());

    mqtt_app_start();
}
