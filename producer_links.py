import requests
from bs4 import BeautifulSoup
import pika


def fetch_links(url):
    # Добавляем протокол, если его нет
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'http://' + url

    try:
        response = requests.get(url, timeout=10)  # Устанавливаем таймаут
        response.raise_for_status()  # Проверяем статус ответа
        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлечение ссылок
        links = []
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            # Внутренние ссылки
            if href.startswith('/') or url in href:
                full_link = href if href.startswith('http') else url.rstrip('/') + '/' + href.lstrip('/')
                links.append(full_link)
        return links
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return []


def send_links_to_queue(links):
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
        channel = connection.channel()

        channel.queue_declare(queue='link_queue')
        for link in links:
            channel.basic_publish(exchange='', routing_key='link_queue', body=link)
            print(f" [x] Sent {link}")
        connection.close()
    except Exception as e:
        print(f"Error sending links to RabbitMQ: {e}")


if __name__ == "__main__":
    target_url = input("Enter the URL to scrape: ").strip()
    links = fetch_links(target_url)
    if links:
        print(f"Found {len(links)} links:")
        for link in links:
            print(f" - {link}")
        send_links_to_queue(links)
    else:
        print("No links found or an error occurred.")
