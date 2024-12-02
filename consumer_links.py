import asyncio
import aiohttp
import os
import pika
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "links_queue"
PROCESSED_LINKS_FILE = "processed_links.txt"
TIMEOUT = 10  # Таймаут в секундах, после которого консумер завершает работу


def is_internal_link(base_url, link):
    return urlparse(link).netloc == urlparse(base_url).netloc or not urlparse(link).netloc


async def fetch_links(url):
    """Асинхронное получение всех ссылок с HTML-страницы."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return []
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                links = set()
                for tag in soup.find_all("a", href=True):
                    href = tag.get("href")
                    full_url = urljoin(url, href)
                    links.add(full_url)
                return links
    except Exception as e:
        print(f"Error fetching links from {url}: {e}")
        return []


async def process_message(channel, body):
    """Обрабатывает сообщение из очереди."""
    url = body.decode()
    print(f"Consumer processing: {url}")
    links = await fetch_links(url)
    base_url = urlparse(url).scheme + "://" + urlparse(url).netloc

    for link in links:
        if is_internal_link(base_url, link):
            with open(PROCESSED_LINKS_FILE, "a+") as f:
                f.seek(0)
                if link not in f.read():
                    f.write(link + "\n")
                    channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=link)
                    print(f"Consumer added to queue: {link}")


async def consume():
    """Запуск консумера."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    async def consume_queue():
        while True:
            method_frame, _, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
            if body:
                await process_message(channel, body)
            else:
                print("Queue is empty, waiting...")
                await asyncio.sleep(TIMEOUT)
                break

    await consume_queue()
    connection.close()


if __name__ == "__main__":
    asyncio.run(consume())
