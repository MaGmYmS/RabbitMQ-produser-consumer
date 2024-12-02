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
                # Проверяем, относится ли ссылка к домену
                if base_domain in full_link:
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


async def process_link(url, session, channel, queue_name, visited, visited_file):
    """Обрабатывает одну ссылку, извлекает из неё другие ссылки и добавляет их в очередь."""
    if url in visited:
        return  # Пропускаем уже посещённую ссылку

    # Добавляем ссылку в файл с посещёнными ссылками
    with open(visited_file, 'a') as f:
        f.write(url + '\n')
    visited.add(url)

    print(f"Processing {url}")
    # Получаем ссылки на текущей странице
    links = await fetch_links(url, session, base_domain=url)
    await send_links_to_queue(links, channel, queue_name, visited)

    return links


async def consume_and_process(queue, session, channel, queue_name, visited, visited_file):
    """Потребляет сообщения из очереди и обрабатывает каждую ссылку."""
    to_visit = []

    # Получаем начальные ссылки из очереди
    while True:
        message = await queue.get()
        async with message.process():
            link = message.body.decode()
            print(f"Got link from queue: {link}")
            to_visit.append(link)

        # После того как начальные ссылки собраны, начинаем их обработку
        while to_visit:
            url = to_visit.pop(0)  # Берём следующую ссылку из очереди
            if url in visited:
                continue  # Пропускаем, если ссылка уже была посещена

            # Обрабатываем текущую ссылку
            print(f"Processing {url}")
            new_links = await process_link(url, session, channel, queue_name, visited, visited_file)

            # Добавляем новые ссылки в очередь для дальнейшей обработки
            for new_link in new_links:
                if new_link not in visited:
                    to_visit.append(new_link)  # Добавляем в очередь
                    visited.add(new_link)  # Помечаем как посещённую

            # Чтобы не загружать память слишком сильно, можем ограничить количество ссылок
            # и делать паузу между обработкой страниц, если это необходимо
            await asyncio.sleep(1)  # Пауза для избежания излишней нагрузки на сервер


async def main():
    url = input("Enter the URL to scrape: ").strip()
    url = ensure_url_scheme(url)  # Добавляем схему, если её нет
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    visited = set()  # Множество для хранения посещённых ссылок
    visited_file = "visited_links.txt"  # Файл для хранения посещённых ссылок

    # Загружаем уже посещённые ссылки из файла (если он существует)
    if os.path.exists(visited_file):
        with open(visited_file, 'r') as f:
            visited = set(f.read().splitlines())

    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue_name = "link_queue"
        await channel.declare_queue(queue_name, durable=False)

        # Добавляем начальную ссылку в очередь
        await channel.default_exchange.publish(
            aio_pika.Message(body=url.encode()),
            routing_key=queue_name,
        )

        # Ожидаем и обрабатываем ссылки из очереди
        async with ClientSession() as session:
            queue = await channel.get_queue(queue_name)
            await consume_and_process(queue, session, channel, queue_name, visited, visited_file)


if __name__ == "__main__":
    asyncio.run(main())
