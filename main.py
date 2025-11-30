#!/usr/bin/env python3
import board
import time
import adafruit_dht
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
import os
from typing import Optional


class AppContext:
    """Application context holding database credentials and connection pool."""

    def __init__(self, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        # Create connection string
        self.conninfo = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"

        # Initialize connection pool
        self.pool: Optional[ConnectionPool] = None

    def init_pool(self, min_size: int = 2, max_size: int = 10):
        """Initialize the connection pool."""
        self.pool = ConnectionPool(
            conninfo=self.conninfo,
            min_size=min_size,
            max_size=max_size
        )

    def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.close()

    def create_table_if_not_exists(self):
        """Create the readings table if it doesn't exist."""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS readings (
                        id SERIAL PRIMARY KEY,
                        temperature FLOAT NOT NULL,
                        humidity FLOAT NOT NULL,
                        recorded_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                    """
                )
            conn.commit()

    @classmethod
    def from_env(cls) -> "AppContext":
        """Create AppContext from environment variables."""
        load_dotenv()

        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "humidityd")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "")

        return cls(db_host, db_port, db_name, db_user, db_password)


def main():
    # Initialize app context with database credentials
    ctx = AppContext.from_env()
    ctx.init_pool()

    # Ensure the readings table exists
    ctx.create_table_if_not_exists()

    try:
        # DHT22 on GPIO4, specific to how my pi5 is wired
        dht_device = adafruit_dht.DHT22(board.D4)  # type: ignore[reportArgumentType]
        while True:
            try:
                temperature = dht_device.temperature
                humidity = dht_device.humidity
                print(f"Temprature: {temperature}")
                print(f"Humidity: {humidity}")

                # Write readings to database
                if temperature is not None and humidity is not None:
                    write_readings(ctx, temperature, humidity)

            except RuntimeError as error:
                # Errors happen fairly often, DHT's are hard to read, just keep going
                print(error.args[0])
                time.sleep(2.0)
                continue
            except Exception as error:
                dht_device.exit()
                raise error
            time.sleep(300.0)
    finally:
        ctx.close_pool()


def write_readings(ctx: AppContext, temp: float, hum: float):
    """Write temperature and humidity readings to the database."""
    if not ctx.pool:
        raise RuntimeError("Connection pool not initialized")

    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO readings (temperature, humidity, recorded_at)
                VALUES (%s, %s, NOW())
                """,
                (temp, hum)
            )
        conn.commit()


if __name__ == "__main__":
    main()
