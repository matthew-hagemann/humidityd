#!/usr/bin/env python3
import board
import time
import adafruit_dht

def main():
    # DHT22 on GPIO4, specific to how my pi5 is wired
    dht_device = adafruit_dht.DHT22(board.D4, use_pulseio=False)  # type: ignore[reportArgumentType]
    while True:
        try:
            temperature = dht_device.temperature
            humidity = dht_device.humidity
            print(f"Temprature: {temperature}")
            print(f"Humidity: {humidity}")
        except RuntimeError as error:
            # Errors happen fairly often, DHT's are hard to read, just keep going
            print(error.args[0])
            time.sleep(2.0)
            continue
        except Exception as error:
            dht_device.exit()
            raise error
        time.sleep(2.0)
