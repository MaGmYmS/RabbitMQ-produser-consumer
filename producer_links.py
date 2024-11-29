import asyncio
import os
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import aio_pika

# Глобальный счётчик обработанных ссылок
processed_links_count = 0


def ensure_url_scheme(url):
    """Добавляет схему http://, если она отсутствует."""
    if not url.startswith(("http://", "https://")):
        return f"http://{url}"
    return url


async def fetch_links(url, session, base_domain):
    """Получает ссылки с указанного URL."""
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Failed to fetch {url}: HTTP {response.status}")
                return []
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            links = []
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                # Формируем полный URL
                full_link = href if href.startswith('http') else url.rstrip('/') + '/' + href.lstrip('/')
                # print(base_domain, full_link)
                if base_domain in full_link:  # Проверяем, относится ли ссылка к домену
                    links.append(full_link)
            return links
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return []


async def send_links_to_queue(links, channel, queue_name, visited):
    """Отправляет ссылки в очередь RabbitMQ через канал."""
    global processed_links_count
    for link in links:
        if link not in visited:  # Отправляем только необработанные ссылки
            await channel.default_exchange.publish(
                aio_pika.Message(body=link.encode()),
                routing_key=queue_name,
            )
            visited.add(link)  # Помечаем ссылку как обработанную
            processed_links_count += 1  # Увеличиваем счётчик
            print(f" [x] Sent {link} (Total processed links: {processed_links_count})")


async def process_links(start_url, session, channel, queue_name, visited):
    """Рекурсивно обрабатывает ссылки."""
    to_visit = [start_url]
    while to_visit:
        url = to_visit.pop(0)  # Берем следующую ссылку для обработки
        if url in visited:
            continue

        print(f"Processing {url}")
        visited.add(url)

        # Получаем ссылки на текущей странице
        links = await fetch_links(url, session, base_domain=start_url)
        await send_links_to_queue(links, channel, queue_name, visited)

        # Добавляем новые ссылки в очередь для посещения
        to_visit.extend([link for link in links if link not in visited])


async def main():
    url = input("Enter the URL to scrape: ").strip()
    url = ensure_url_scheme(url)  # Добавляем схему, если её нет
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    visited = set()  # Множество для хранения посещённых ссылок

    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue_name = "link_queue"
        await channel.declare_queue(queue_name, durable=False)

        async with ClientSession() as session:
            await process_links(url, session, channel, queue_name, visited)


if __name__ == "__main__":
    asyncio.run(main())
