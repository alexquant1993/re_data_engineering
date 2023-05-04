import httpx

headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "es-ES,es;q=0.9",
                "referer": "https://www.google.com/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            }
session = httpx.Client(headers=headers, timeout=60, follow_redirects=True)
url = "https://www.idealista.com/venta-viviendas/madrid-madrid/con-publicado_ultimas-24-horas/"
response = session.get(url)
response.status_code