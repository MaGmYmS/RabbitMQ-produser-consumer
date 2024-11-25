import asyncio
import os
import aio_pika

# Глобальная переменная для исходного таймаута
timeout_env = int(os.getenv("QUEUE_TIMEOUT", 10))  # Таймаут в секундах


async def process_link(link):
    """Обработка полученной ссылки."""
    print(f" [x] Processing link: {link}")


async def consume(queue):
    """Потребляет сообщения из очереди с завершением при таймауте."""
    timeout_counter = timeout_env  # Таймер для завершения при отсутствии сообщений

    try:
        while True:
            try:
                # Ожидаем сообщение из очереди с заданным таймаутом
                incoming_message = await asyncio.wait_for(queue.get(), timeout=timeout_counter)
                async with incoming_message.process():
                    link = incoming_message.body.decode()
                    await process_link(link)

                # Сбрасываем таймер после успешной обработки
                timeout_counter = timeout_env

            except asyncio.TimeoutError:
                print(f" [!] No messages for {timeout_env} seconds. Exiting consumer...")
                break
            except aio_pika.exceptions.QueueEmpty:
                print(" [!] Queue is empty. Waiting...")
                await asyncio.sleep(1)  # Короткая пауза перед повторной проверкой
                timeout_counter -= 1
                if timeout_counter <= 0:
                    print(f" [!] Timeout of {timeout_env} seconds reached. Exiting consumer...")
                    break
            except Exception as e:
                print(f" [!] Error during message processing: {e}")
                break
    except asyncio.CancelledError:
        print(" [!] Consumer cancelled.")
    except Exception as e:
        print(f" [!] Unexpected error in consumer: {e}")


async def main():
    """Основная функция."""
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    try:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("link_queue", durable=False)

            print(" [*] Waiting for messages...")
            await consume(queue)
    except Exception as e:
        print(f" [!] Error in main function: {e}")


if __name__ == "__main__":
    asyncio.run(main())
