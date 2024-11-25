import os
import pika


def process_link(link):
    print(f" [x] Processing link: {link}")


def callback(ch, method, properties, body):
    link = body.decode()
    process_link(link)


if __name__ == "__main__":
    # Получаем параметры подключения из переменных окружения
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
    rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
    rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "guest")

    # Создаём подключение с использованием этих параметров
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, '/', credentials)
    )
    channel = connection.channel()

    # Создаём очередь
    channel.queue_declare(queue='link_queue')
    channel.basic_consume(queue='link_queue', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for links.')
    channel.start_consuming()
