import asyncio
import os
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import aio_pika


def ensure_url_scheme(url):
    """Добавляет схему http://, если она отсутствует."""
    if not url.startswith(("http://", "https://")):
        return f"http://{url}"
    return url


async def fetch_links(url, session):
    """Получает ссылки с указанного URL."""
    try:
        async with session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            links = []
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                if href.startswith('/') or url in href:
                    # Формируем полный URL
                    full_link = href if href.startswith('http') else url.rstrip('/') + '/' + href.lstrip('/')
                    links.append(full_link)
            return links
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return []


async def send_links_to_queue(links, channel, queue_name):
    """Отправляет ссылки в очередь RabbitMQ через канал."""
    for link in links:
        await channel.default_exchange.publish(
            aio_pika.Message(body=link.encode()),
            routing_key=queue_name,
        )
        print(f" [x] Sent {link}")


async def main():
    url = input("Enter the URL to scrape: ").strip()
    url = ensure_url_scheme(url)  # Добавляем схему, если её нет
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue_name = "link_queue"
        await channel.declare_queue(queue_name, durable=False)

        async with ClientSession() as session:
            links = await fetch_links(url, session)
            if links:
                print(f"Found {len(links)} links.")
                await send_links_to_queue(links, channel, queue_name)
            else:
                print("No links found or an error occurred.")


if __name__ == "__main__":
    asyncio.run(main())
