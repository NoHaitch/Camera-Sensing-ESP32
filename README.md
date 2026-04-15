<br />
<div align="center">
  <h1 align="center">Camera Sensing ESP32 </h1>

  <p align="center">
    <h3> Simulation of Camera Sensing IOT using ESP32 to send Image using MQTT to cloud broker and receive using Local Python Server</h3>
    <br/>
  </p>
</div>

<div align="center" id="contributor">
  <strong>
    <h3>Made By:</h3>
    <h3>Raden Francisco Trianto B.</h3>
    <h3>NIM 13522091</h3>
  </strong>
  <br>
</div>

## About The Project

This project simulates sending image data from an ESP32 device to a cloud server using MQTT protocol. In this setup, MQTT broker acts as the cloud messaging system, but the server/user/receiver is local.

The ESP32 device captures or has an image and sends it to the server via MQTT in two separate topics:
- `/raw`: Contains the actual raw image data
- `/meta`: Contains metadata about the image (e.g., size, format, timestamp, etc.)

The receiver is a Python script that simulates the cloud server, subscribing to these topics and processing the received data.

## Project Structure

```
root/
├── ESP32/
│   ├── CMakeLists.txt
│   ├── sdkconfig
│   ├── sdkconfig.defaults
│   └── main/
│       ├── app_main.c
│       ├── CMakeLists.txt
│       ├── idf_component.yml
│       └── Kconfig.projbuild
└── receiver/
    ├── mqtt_receiver.py
    ├── mqttcred
    └── received_images/
```

### Note: 
- `ESP32/`: ESP-IDF project for the ESP32 microcontroller
- `receiver/`: Contains Python server as receiver simulation


## Setup and Usage

### ESP32 Setup
1. Navigate to the `ESP32/` directory
2. Configure the project using `idf.py menuconfig`. You will need to configure wifi SSID and MQTT config.
3. Build the project: `idf.py build`
4. Flash to ESP32: `idf.py flash`
5. Monitor: `idf.py monitor`

### Receiver Setup
1. Requires Python
2. Install required package 
    ```
    pip install paho-mqtt
    ```
3. Run the receiver: 
    ```
    python mqtt_receiver.py \
      --host HOST \
      --port PORT \
      --username USERNAME \
      --password PASSWORD \
      --tls
    ```
