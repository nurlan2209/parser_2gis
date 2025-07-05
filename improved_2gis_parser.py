import asyncio
import logging
import random
import re
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from playwright.async_api import async_playwright, Page, Browser
import argparse
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('2gis_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GISParser:
    def __init__(self, city: str = "Астана", max_items_per_category: int = 100):
        self.city = city
        self.max_items_per_category = max_items_per_category
        self.results = []
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        
    async def setup_browser(self):
        """Настройка и запуск браузера"""
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-features=VizDisplayCompositor',
                '--ignore-certificate-errors',
                '--ignore-ssl-errors',
                '--ignore-certificate-errors-spki-list'
            ]
        )
        
        context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 920},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            java_script_enabled=True,
            ignore_https_errors=True
        )
        
        self.page = await context.new_page()
        
        # Увеличиваем таймауты
        self.page.set_default_timeout(30000)
        self.page.set_default_navigation_timeout(30000)
        
        # Блокируем только тяжелые ресурсы
        await self.page.route("**/*.{png,jpg,jpeg,gif,webp,svg,mp4,avi,mov}", lambda route: route.abort())
        
        logger.info("Браузер успешно запущен")
        
    async def random_delay(self, min_sec: float = 1.0, max_sec: float = 3.0):
        """Случайная задержка"""
        delay = random.uniform(min_sec, max_sec)
        logger.debug(f"Задержка {delay:.1f} секунд")
        await asyncio.sleep(delay)
        
    async def open_2gis_and_search(self, category: str):
        """Открытие 2ГИС и выполнение поиска"""
        try:
            # Пробуем разные варианты URL
            urls_to_try = [
                f"https://2gis.kz/{self.city.lower()}",
                f"https://2gis.kz/astana",
                "https://2gis.kz"
            ]
            
            page_loaded = False
            for url in urls_to_try:
                try:
                    logger.info(f"Пробуем загрузить: {url}")
                    await self.page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    await self.random_delay(2, 4)
                    page_loaded = True
                    logger.info(f"Успешно загружена страница: {url}")
                    break
                except Exception as e:
                    logger.warning(f"Не удалось загрузить {url}: {e}")
                    continue
            
            if not page_loaded:
                logger.error("Не удалось загрузить ни одну страницу 2ГИС")
                return False
            
            # Ищем поле поиска с различными селекторами
            search_selectors = [
                'input[placeholder*="Поиск"]',
                'input[placeholder*="поиск"]',
                'input[type="search"]',
                'input[name="search"]',
                '.search-input input',
                'input'
            ]
            
            search_input = None
            for selector in search_selectors:
                try:
                    search_input = await self.page.wait_for_selector(selector, timeout=5000)
                    if search_input:
                        logger.info(f"Найдено поле поиска: {selector}")
                        break
                except:
                    continue
            
            if not search_input:
                logger.error("Не удалось найти поле поиска")
                return False
            
            # Вводим поисковый запрос
            await search_input.fill('')
            await self.random_delay(0.5, 1)
            await search_input.type(category, delay=100)
            await self.random_delay(1, 2)
            
            # Нажимаем Enter
            await self.page.keyboard.press('Enter')
            await self.random_delay(5, 7)
            
            # Проверяем, что поиск выполнился
            current_url = self.page.url
            if category.lower() in current_url.lower() or 'search' in current_url.lower():
                logger.info(f"Поиск выполнен успешно: {category}")
                return True
            else:
                logger.warning(f"Поиск может быть не выполнен. Текущий URL: {current_url}")
                # Даем еще одну попытку
                await self.random_delay(3, 5)
                return True
            
        except Exception as e:
            logger.error(f"Ошибка при поиске: {e}")
            return False
            
    async def get_business_links_pagination_fixed(self):
        """ИСПРАВЛЕННАЯ пагинация - обрабатывает ВСЕ страницы"""
        try:
            await self.random_delay(3, 5)
            
            logger.info("🔍 Запуск ИСПРАВЛЕННОЙ пагинации...")
            
            all_unique_links = set()
            current_page = 1
            max_pages = 50  # Увеличиваем лимит
            consecutive_failures = 0
            max_failures = 3  # Если 3 раза подряд не можем перейти - останавливаемся
            
            while current_page <= max_pages and consecutive_failures < max_failures:
                logger.info(f"📄 Обрабатываем страницу {current_page}")
                
                # Скроллим вверх перед сбором ссылок
                await self.page.evaluate("window.scrollTo(0, 0)")
                await self.random_delay(2, 3)
                
                # Собираем ссылки с текущей страницы
                current_links = await self.collect_links_from_current_page()
                old_count = len(all_unique_links)
                all_unique_links.update(current_links)
                new_count = len(all_unique_links)
                new_links = new_count - old_count
                
                logger.info(f"📊 Страница {current_page}: добавлено {new_links} новых ссылок (всего: {new_count})")
                
                # Проверяем лимит
                if new_count >= self.max_items_per_category:
                    logger.info(f"🎯 Достигнут лимит {self.max_items_per_category} ссылок!")
                    break
                
                # Если нет новых ссылок, но это не первая страница - возможно конец
                if new_links == 0 and current_page > 1:
                    logger.info("❌ Нет новых ссылок - возможно, это последняя страница")
                    break
                
                # Переходим на следующую страницу
                next_page_found = await self.go_to_next_page_fixed(current_page + 1)
                
                if next_page_found:
                    consecutive_failures = 0  # Сбрасываем счетчик неудач
                    current_page += 1
                else:
                    consecutive_failures += 1
                    logger.info(f"⚠️ Не удалось перейти на страницу {current_page + 1} (попытка {consecutive_failures}/{max_failures})")
                    
                    if consecutive_failures >= max_failures:
                        logger.info("🏁 Больше нет доступных страниц")
                        break
            
            business_urls = list(all_unique_links)[:self.max_items_per_category]
            logger.info(f"🎉 ИТОГО собрано {len(business_urls)} ссылок с {current_page} страниц!")
            
            return business_urls
            
        except Exception as e:
            logger.error(f"💥 Ошибка в исправленной пагинации: {e}")
            return []

    async def collect_links_from_current_page(self):
        """Собираем все ссылки с текущей страницы"""
        links = set()
        
        # Ждем загрузки контента
        await self.random_delay(2, 4)
        
        link_selectors = [
            'a[href*="/firm/"]',
            'a[href*="/organization/"]',
            'a[href*="/branch/"]',
            'a[href*="astana/firm"]',
            'a[href*="astana/organization"]',
            'a[href*="astana/branch"]'
        ]
        
        for selector in link_selectors:
            try:
                elements = await self.page.query_selector_all(selector)
                for element in elements:
                    href = await element.get_attribute('href')
                    if href:
                        if any(word in href for word in ['firm', 'organization', 'branch']):
                            if href.startswith('/'):
                                full_url = f"https://2gis.kz{href}"
                            elif href.startswith('http'):
                                full_url = href
                            else:
                                continue
                            links.add(full_url)
            except:
                continue
        
        return links

    async def go_to_next_page_fixed(self, page_number):
        """ИСПРАВЛЕННЫЙ переход на следующую страницу"""
        try:
            logger.info(f"🔍 Ищем кнопку страницы {page_number}...")
            
            # Сначала скроллим вниз чтобы пагинация была видна
            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await self.random_delay(1, 2)
            
            # Ищем конкретную страницу
            pagination_selectors = [
                f'a:has-text("{page_number}")',
                f'button:has-text("{page_number}")',
                f'[data-page="{page_number}"]',
            ]
            
            pagination_element = None
            
            # Ищем точную кнопку страницы
            for selector in pagination_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        text = await element.text_content()
                        if text and text.strip() == str(page_number):
                            is_visible = await element.is_visible()
                            if is_visible:
                                pagination_element = element
                                logger.info(f"✅ Найдена кнопка страницы {page_number}: {selector}")
                                break
                    if pagination_element:
                        break
                except:
                    continue
            
            # Если не нашли точную кнопку, ищем кнопку "Следующая" или ">"
            if not pagination_element:
                logger.info(f"🔍 Не найдена кнопка {page_number}, ищем 'Следующая'...")
                
                next_selectors = [
                    'a:has-text(">")',
                    'button:has-text(">")', 
                    'a:has-text("Следующая")',
                    'button:has-text("Следующая")',
                    '[class*="next"]',
                    '[aria-label*="Next"]',
                    '[aria-label*="Следующая"]'
                ]
                
                for selector in next_selectors:
                    try:
                        element = await self.page.query_selector(selector)
                        if element:
                            is_visible = await element.is_visible()
                            is_enabled = await element.is_enabled()
                            if is_visible and is_enabled:
                                pagination_element = element
                                logger.info(f"✅ Найдена кнопка 'Следующая': {selector}")
                                break
                    except:
                        continue
            
            # Если нашли элемент - кликаем
            if pagination_element:
                try:
                    # Скроллим к элементу
                    await pagination_element.scroll_into_view_if_needed()
                    await self.random_delay(1, 2)
                    
                    # Кликаем
                    await pagination_element.click()
                    logger.info(f"🎯 Кликнули на страницу {page_number}")
                    
                    # Ждем загрузки
                    await self.random_delay(4, 6)
                    
                    # Проверяем что страница изменилась
                    await self.page.wait_for_load_state('networkidle', timeout=10000)
                    
                    return True
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при клике: {e}")
                    return False
            else:
                # Последняя попытка - ищем ВСЕ элементы пагинации
                logger.info("🔍 Последняя попытка - ищем все элементы пагинации...")
                
                try:
                    # Ищем все возможные элементы пагинации
                    all_pagination = await self.page.query_selector_all('a, button')
                    
                    for element in all_pagination:
                        try:
                            text = await element.text_content()
                            if text and text.strip() == str(page_number):
                                is_visible = await element.is_visible()
                                is_enabled = await element.is_enabled()
                                
                                if is_visible and is_enabled:
                                    logger.info(f"✅ Найден элемент пагинации в общем поиске!")
                                    await element.scroll_into_view_if_needed()
                                    await self.random_delay(1, 2)
                                    await element.click()
                                    await self.random_delay(4, 6)
                                    await self.page.wait_for_load_state('networkidle', timeout=10000)
                                    return True
                        except:
                            continue
                            
                except:
                    pass
                
                logger.info(f"❌ Кнопка страницы {page_number} не найдена")
                return False
                
        except Exception as e:
            logger.error(f"💥 Ошибка при поиске пагинации: {e}")
            return False
            
    async def extract_business_info(self, url: str, category: str) -> Optional[Dict]:
        """Улучшенное извлечение информации с ожиданием динамического контента"""
        try:
            logger.info(f"Переходим на страницу: {url}")
            
            # Переходим на страницу
            await self.page.goto(url, wait_until="domcontentloaded", timeout=20000)
            
            # Ждем загрузки динамического контента
            await self.wait_for_dynamic_content()
            
            # Извлекаем информацию
            name = None
            address = None
            phone = None
            website = None
            whatsapp = None
            instagram = None
            
            try:
                name = await self.extract_text_by_selectors([
                    'h1', 'h2', '[class*="title"]', '[class*="name"]', '[class*="header"]'
                ])
            except Exception as e:
                logger.debug(f"Ошибка при извлечении названия: {e}")
            
            try:
                address = await self.extract_address()
            except Exception as e:
                logger.debug(f"Ошибка при извлечении адреса: {e}")
            
            try:
                phone = await self.extract_phone()
            except Exception as e:
                logger.debug(f"Ошибка при извлечении телефона: {e}")
            
            try:
                website = await self.extract_website()  # Используем улучшенную версию
            except Exception as e:
                logger.debug(f"Ошибка при извлечении сайта: {e}")
            
            try:
                whatsapp = await self.extract_whatsapp()
            except Exception as e:
                logger.warning(f"Ошибка при извлечении WhatsApp: {e}")
            
            try:
                instagram = await self.extract_instagram()
            except Exception as e:
                logger.debug(f"Ошибка при извлечении Instagram: {e}")
            
            result = {
                'Название': name or 'Не указано',
                'Адрес': address or 'Не указано',
                'Телефон': phone or 'Не указано', 
                'Сайт': website or 'Не указано',
                'WhatsApp': whatsapp or 'Не указано',
                'Instagram': instagram or 'Не указано',
                'Категория': category,
                'Есть сайт': 'Да' if website and website != 'Не указано' else 'Нет'
            }
            
            logger.info(f"Собрана информация: {result['Название']}, {result['Адрес']}, {result['Телефон']}")
            if website and website != 'Не указано':
                logger.info(f"Сайт: {result['Сайт']}")
            if whatsapp and whatsapp != 'Не указано':
                logger.info(f"WhatsApp: {result['WhatsApp']}")
            if instagram and instagram != 'Не указано':
                logger.info(f"Instagram: {result['Instagram']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Критическая ошибка при извлечении информации с {url}: {e}")
            return None
            
    async def extract_text_by_selectors(self, selectors: List[str]) -> Optional[str]:
        """Извлечение текста по списку селекторов"""
        for selector in selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text and text.strip():
                        return text.strip()
            except:
                continue
        return None
        
    async def extract_address(self) -> Optional[str]:
        """Извлечение адреса"""
        # Сначала пробуем стандартные селекторы
        address_selectors = [
            '[class*="address"]',
            '[class*="location"]', 
            '.address',
            '.location'
        ]
        
        address = await self.extract_text_by_selectors(address_selectors)
        if address:
            return address
            
        # Если не нашли, ищем в тексте страницы
        try:
            page_text = await self.page.text_content('body')
            if page_text:
                # Паттерны для поиска адреса
                address_patterns = [
                    r'ЖК\s+[А-Яа-я\s]+,\s*улица\s+[А-Яа-я\s]+,\s*\d+',
                    r'улица\s+[А-Яа-я\s]+,\s*\d+[А-Яа-я\s]*',
                    r'проспект\s+[А-Яа-я\s]+,\s*\d+[А-Яа-я\s]*',
                    r'бульвар\s+[А-Яа-я\s]+,\s*\d+[А-Яа-я\s]*',
                    r'[А-Яа-я\s]+(район|микрорайон)[А-Яа-я\s\d,]*',
                    r'Астана[,\s]+[А-Яа-я\s\d,]+'
                ]
                
                for pattern in address_patterns:
                    match = re.search(pattern, page_text)
                    if match:
                        return match.group().strip()
        except:
            pass
            
        return None
        
    async def extract_phone(self) -> Optional[str]:
        """Извлечение телефона"""
        # Ищем кнопки и ссылки с телефонами
        phone_selectors = [
            'a[href^="tel:"]',
            '[class*="phone"]',
            'button[class*="phone"]'
        ]
        
        for selector in phone_selectors:
            try:
                elements = await self.page.query_selector_all(selector)
                for element in elements:
                    # Проверяем href
                    href = await element.get_attribute('href')
                    if href and href.startswith('tel:'):
                        return href.replace('tel:', '').strip()
                    
                    # Проверяем текст
                    text = await element.text_content()
                    if text and re.search(r'[\d\-\+\(\)\s]{7,}', text):
                        return text.strip()
            except:
                continue
                
        # Ищем в тексте страницы
        try:
            page_text = await self.page.text_content('body')
            if page_text:
                phone_patterns = [
                    r'\+7\s*[-\(\s]*\d{3}\s*[-\)\s]*\d{3}[-\s]*\d{2}[-\s]*\d{2}',
                    r'8\s*[-\(\s]*\d{3}\s*[-\)\s]*\d{3}[-\s]*\d{2}[-\s]*\d{2}',
                    r'\+7\d{10}',
                    r'8\d{10}'
                ]
                
                for pattern in phone_patterns:
                    match = re.search(pattern, page_text)
                    if match:
                        return match.group().strip()
        except:
            pass
            
        return None

    async def wait_for_dynamic_content(self):
        """Улучшенное ожидание загрузки динамического контента"""
        try:
            logger.debug("Ждем загрузки динамического контента...")
            
            # Ждем загрузки сети
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            
            # Дополнительная задержка для JavaScript
            await asyncio.sleep(5)
            
            # Ждем появления ключевых элементов
            key_selectors = [
                'h1, h2',  # Заголовок
                '[class*="contact"]',  # Контактная информация
                '[class*="phone"]',   # Телефон
                'button, a'  # Кнопки и ссылки
            ]
            
            for selector in key_selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=3000)
                    logger.debug(f"Найден элемент: {selector}")
                    break
                except:
                    continue
            
            # Еще немного подождем для полной загрузки
            await asyncio.sleep(2)
            
            logger.debug("Динамический контент загружен")
            
        except Exception as e:
            logger.debug(f"Таймаут при ожидании динамического контента: {e}")

    async def decode_2gis_website_link(self, link: str) -> Optional[str]:
        """Простое декодирование 2ГИС ссылок"""
        try:
            import base64
            
            if 'link.2gis.com' not in link:
                return None
                
            logger.debug(f"Декодируем 2ГИС ссылку: {link}")
            
            # Извлекаем base64 часть
            parts = link.split('/')
            if len(parts) < 2:
                return None
                
            encoded_part = parts[-1]
            
            # Удаляем query параметры
            if '?' in encoded_part:
                encoded_part = encoded_part.split('?')[0]
            if '#' in encoded_part:
                encoded_part = encoded_part.split('#')[0]
                
            try:
                # Пробуем декодировать с разными padding
                for padding in ['', '=', '==', '===']:
                    try:
                        padded_data = encoded_part + padding
                        decoded_bytes = base64.b64decode(padded_data)
                        decoded_string = decoded_bytes.decode('utf-8')
                        
                        # Ищем только полные домены
                        domain_patterns = [
                            r'\b([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:kz|com|ru|org|net))\b'
                        ]
                        
                        for pattern in domain_patterns:
                            matches = re.findall(pattern, decoded_string, re.IGNORECASE)
                            for domain in matches:
                                # Исключаем служебные домены
                                if not any(bad in domain.lower() for bad in ['2gis', 'sberbank', 'yandex', 'google']):
                                    if len(domain) > 6:
                                        result = f"https://{domain}"
                                        logger.info(f"Декодирован сайт: {result}")
                                        return result
                        
                        break
                    except:
                        continue
                        
            except Exception as e:
                logger.debug(f"Ошибка декодирования: {e}")
                
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка при декодировании ссылки: {e}")
            return None

    async def extract_website(self) -> Optional[str]:
        """Простое извлечение сайта только по классу _49kxlr"""
        try:
            logger.info("Ищем сайт только в элементах с классом _49kxlr...")

            # Ищем элементы с классом _49kxlr
            try:
                elements = await self.page.query_selector_all('._49kxlr')
                logger.debug(f"Найдено элементов с классом _49kxlr: {len(elements)}")
                
                for element in elements:
                    try:
                        # Ищем ссылки внутри элемента
                        links = await element.query_selector_all('a')
                        for link in links:
                            # Получаем текст ссылки
                            link_text = await link.text_content()
                            if link_text:
                                link_text = link_text.strip()
                                logger.debug(f"Найден текст ссылки: {link_text}")
                                
                                # Проверяем, что это домен
                                if re.match(r'^[a-zA-Z0-9][-a-zA-Z0-9]*\.(?:kz|com|ru|org|net|biz|cafe|coffee)$', link_text):
                                    # Проверяем длину домена
                                    if len(link_text) > 6:
                                        result = f"https://{link_text}"
                                        logger.info(f"Найден сайт в _49kxlr: {result}")
                                        return result
                            
                            # Также проверяем href на случай если там прямая ссылка
                            href = await link.get_attribute('href')
                            if href and 'link.2gis.com' in href:
                                # Попробуем декодировать 2ГИС ссылку
                                decoded_site = await self.decode_2gis_website_link(href)
                                if decoded_site:
                                    logger.info(f"Декодирован сайт из _49kxlr: {decoded_site}")
                                    return decoded_site
                                    
                    except Exception as e:
                        logger.debug(f"Ошибка при обработке элемента _49kxlr: {e}")
                        continue
                        
            except Exception as e:
                logger.debug(f"Ошибка при поиске элементов _49kxlr: {e}")

            # Если не нашли в _49kxlr, пробуем похожие классы
            try:
                similar_selectors = [
                    '[class*="49kxlr"]',
                    '[class*="kxlr"]',
                    '[class*="_rehek"]',  # Из вашего примера был класс _1rehek
                    '[class*="rehek"]'
                ]
                
                for selector in similar_selectors:
                    try:
                        elements = await self.page.query_selector_all(selector)
                        logger.debug(f"Найдено элементов с селектором {selector}: {len(elements)}")
                        
                        for element in elements:
                            links = await element.query_selector_all('a')
                            for link in links:
                                link_text = await link.text_content()
                                if link_text:
                                    link_text = link_text.strip()
                                    
                                    # Строгая проверка на домен
                                    if re.match(r'^[a-zA-Z0-9][-a-zA-Z0-9]*\.(?:kz|com|ru|org|net)$', link_text):
                                        if len(link_text) > 6 and '2gis' not in link_text.lower():
                                            result = f"https://{link_text}"
                                            logger.info(f"Найден сайт в {selector}: {result}")
                                            return result
                                            
                    except Exception as e:
                        logger.debug(f"Ошибка при поиске {selector}: {e}")
                        continue
                        
            except Exception as e:
                logger.debug(f"Ошибка при поиске похожих селекторов: {e}")

            logger.info("Сайт не найден в элементах _49kxlr")
            return None
                
        except Exception as e:
            logger.error(f"Критическая ошибка при поиске сайта: {e}")
            return None
        
    async def decode_2gis_link(self, link: str) -> Optional[str]:
        """Улучшенное декодирование ссылок 2ГИС"""
        try:
            import base64
            import urllib.parse
            
            if 'link.2gis.com' not in link:
                return None
                
            logger.debug(f"Пытаемся декодировать: {link}")
            
            # Извлекаем base64 часть после последнего /
            parts = link.split('/')
            if len(parts) < 2:
                return None
                
            encoded_part = parts[-1]
            
            # Удаляем query параметры если есть
            if '?' in encoded_part:
                encoded_part = encoded_part.split('?')[0]
            if '#' in encoded_part:
                encoded_part = encoded_part.split('#')[0]
                
            try:
                # Пробуем разные варианты декодирования
                for padding in ['', '=', '==', '===']:
                    try:
                        padded_data = encoded_part + padding
                        decoded_bytes = base64.b64decode(padded_data)
                        decoded_string = decoded_bytes.decode('utf-8')
                        
                        # Ищем wa.me ссылку в декодированной строке
                        wa_patterns = [
                            r'(https://wa\.me/[^\s%"\']+)',
                            r'wa\.me/(\d+)',
                            r'whatsapp://send\?phone=(\d+)'
                        ]
                        
                        for pattern in wa_patterns:
                            matches = re.findall(pattern, decoded_string)
                            for match in matches:
                                if match.startswith('https://'):
                                    wa_url = urllib.parse.unquote(match)
                                    logger.info(f"Декодирована ссылка WhatsApp: {wa_url}")
                                    return wa_url
                                elif match.isdigit():
                                    wa_url = f"https://wa.me/{match}"
                                    logger.info(f"Создана ссылка WhatsApp: {wa_url}")
                                    return wa_url
                        
                        break  # Если декодирование прошло успешно, выходим
                    except:
                        continue
                        
            except Exception as e:
                logger.debug(f"Ошибка декодирования base64: {e}")
                
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка при декодировании ссылки 2gis: {e}")
            return None

    async def extract_whatsapp(self) -> Optional[str]:
        """Упрощенное и стабильное извлечение WhatsApp"""
        try:
            # 1. Сначала ищем прямые ссылки на WhatsApp
            direct_selectors = [
                'a[href*="wa.me"]',
                'a[href*="whatsapp"]',
                'a[href*="link.2gis.com"]'
            ]
            
            for selector in direct_selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute('href')
                        if href:
                            if 'wa.me' in href or 'whatsapp' in href:
                                logger.info(f"Найдена прямая ссылка WhatsApp: {href}")
                                return href
                            elif 'link.2gis.com' in href:
                                # Пытаемся декодировать ссылку 2gis
                                decoded = await self.decode_2gis_link(href)
                                if decoded:
                                    return decoded
                except Exception as e:
                    logger.debug(f"Ошибка при поиске {selector}: {e}")
                    continue

            # 2. Ищем кнопки WhatsApp БЕЗ КЛИКА - только проверяем атрибуты
            whatsapp_selectors = [
                'button[title*="WhatsApp"], button[title*="whatsapp"]',
                'a[title*="WhatsApp"], a[title*="whatsapp"]',
                'div[title*="WhatsApp"], div[title*="whatsapp"]',
                '[class*="whatsapp"]',
                '[data-social="whatsapp"]',
                'button:has-text("WhatsApp")',
                'a:has-text("WhatsApp")'
            ]
            
            for selector in whatsapp_selectors:
                try:
                    buttons = await self.page.query_selector_all(selector)
                    for button in buttons:
                        # Проверяем различные атрибуты БЕЗ КЛИКА
                        attributes_to_check = [
                            'href', 'data-url', 'data-link', 'data-phone', 
                            'data-action', 'data-contact', 'onclick',
                            'data-whatsapp', 'data-phone-number'
                        ]
                        
                        for attr in attributes_to_check:
                            try:
                                attr_value = await button.get_attribute(attr)
                                if not attr_value:
                                    continue
                                    
                                # Ищем номер телефона в атрибуте
                                phone_patterns = [
                                    r'(\+?7\d{10})',
                                    r'wa\.me/(\d+)',
                                    r'whatsapp://send\?phone=(\d+)'
                                ]
                                
                                for pattern in phone_patterns:
                                    match = re.search(pattern, attr_value)
                                    if match:
                                        phone = match.group(1).replace('+', '')
                                        if not phone.startswith('7') and len(phone) == 10:
                                            phone = '7' + phone
                                        elif not phone.startswith('7') and len(phone) == 11:
                                            phone = phone
                                        result = f"https://wa.me/{phone}"
                                        logger.info(f"Создана ссылка WhatsApp из {attr}: {result}")
                                        return result
                                
                                # Ищем готовую ссылку WhatsApp
                                if 'wa.me' in attr_value or 'whatsapp' in attr_value:
                                    wa_match = re.search(r'(https://wa\.me/[^\s\'"]+)', attr_value)
                                    if wa_match:
                                        logger.info(f"Найдена ссылка WhatsApp в {attr}: {wa_match.group(1)}")
                                        return wa_match.group(1)
                            except Exception as e:
                                logger.debug(f"Ошибка при проверке атрибута {attr}: {e}")
                                continue
                except Exception as e:
                    logger.debug(f"Ошибка при поиске {selector}: {e}")
                    continue

            # 3. Ищем в исходном коде страницы
            try:
                page_content = await self.page.content()
                
                # Ищем ссылки 2gis с возможным WhatsApp
                gis_patterns = [
                    r'href=["\']([^"\']*link\.2gis\.com[^"\']*)["\']',
                    r'(https://link\.2gis\.com/[^\s\'"]+)'
                ]
                
                for pattern in gis_patterns:
                    matches = re.findall(pattern, page_content)
                    for match in matches:
                        decoded = await self.decode_2gis_link(match)
                        if decoded:
                            return decoded
                
                # Ищем номера телефонов в контексте WhatsApp
                whatsapp_patterns = [
                    r'whatsapp[^0-9]*(\+?7\d{10})',
                    r'wa\.me/(\d+)',
                    r'data-phone["\']:\s*["\'](\+?7\d{10})["\']',
                    r'phone["\']:\s*["\'](\+?7\d{10})["\']',
                    r'whatsapp["\']?\s*:\s*["\']([^"\']+)["\']'
                ]
                
                for pattern in whatsapp_patterns:
                    matches = re.findall(pattern, page_content, re.IGNORECASE)
                    for match in matches:
                        if 'wa.me' in match or 'whatsapp' in match:
                            logger.info(f"Найдена ссылка WhatsApp в коде: {match}")
                            return match
                        elif re.match(r'\+?7?\d{10,11}', match):
                            phone = match.replace('+', '').replace(' ', '').replace('-', '')
                            if len(phone) == 11 and phone.startswith('7'):
                                result = f"https://wa.me/{phone}"
                            elif len(phone) == 10:
                                result = f"https://wa.me/7{phone}"
                            else:
                                continue
                            logger.info(f"Создана ссылка WhatsApp из паттерна: {result}")
                            return result
            except Exception as e:
                logger.debug(f"Ошибка при поиске в исходном коде: {e}")

            # 4. Последняя попытка - ищем любые упоминания номеров рядом с WhatsApp в тексте
            try:
                page_text = await self.page.text_content('body')
                if page_text:
                    # Разбиваем текст по упоминаниям WhatsApp
                    text_lower = page_text.lower()
                    whatsapp_pos = text_lower.find('whatsapp')
                    
                    if whatsapp_pos != -1:
                        # Берем текст в радиусе 200 символов от слова WhatsApp
                        start = max(0, whatsapp_pos - 100)
                        end = min(len(page_text), whatsapp_pos + 100)
                        context = page_text[start:end]
                        
                        # Ищем номер телефона в этом контексте
                        phone_match = re.search(r'(\+?7\d{10})', context)
                        if phone_match:
                            phone = phone_match.group(1).replace('+', '')
                            if not phone.startswith('7'):
                                phone = '7' + phone
                            result = f"https://wa.me/{phone}"
                            logger.info(f"Создана ссылка WhatsApp из контекста: {result}")
                            return result
            except Exception as e:
                logger.debug(f"Ошибка при поиске в тексте: {e}")
                
        except Exception as e:
            logger.error(f"Критическая ошибка при поиске WhatsApp: {e}")
            
        return None
        
    async def extract_instagram(self) -> Optional[str]:
        """Извлечение Instagram - только реальные ссылки"""
        try:
            # Сначала ищем прямые ссылки на Instagram
            instagram_links = await self.page.query_selector_all('a[href*="instagram"]')
            for link in instagram_links:
                href = await link.get_attribute('href')
                if href and 'instagram' in href:
                    logger.info(f"Найдена ссылка Instagram: {href}")
                    return href
                    
            # Ищем кнопки Instagram и проверяем события
            instagram_buttons = await self.page.query_selector_all('button, div, span, a')
            for button in instagram_buttons:
                try:
                    text = await button.text_content()
                    if text and 'instagram' in text.lower():
                        # Проверяем onclick
                        onclick = await button.get_attribute('onclick')
                        if onclick:
                            # Ищем ссылку в onclick
                            instagram_match = re.search(r'(https://[^\'"\s]*instagram\.com[^\'"\s]*)', onclick)
                            if instagram_match:
                                link = instagram_match.group(1)
                                logger.info(f"Найдена ссылка Instagram в onclick: {link}")
                                return link
                        
                        # Проверяем data-атрибуты
                        for attr in ['data-url', 'data-link', 'data-href', 'data-instagram', 'data-action']:
                            attr_value = await button.get_attribute(attr)
                            if attr_value and 'instagram' in attr_value:
                                logger.info(f"Найдена ссылка Instagram в {attr}: {attr_value}")
                                return attr_value
                        
                        # Ищем родительские элементы с ссылками
                        parent = button
                        for _ in range(3):  # Проверяем до 3 уровней вверх
                            try:
                                parent = await parent.query_selector('xpath=..')
                                if parent:
                                    parent_href = await parent.get_attribute('href')
                                    if parent_href and 'instagram' in parent_href:
                                        logger.info(f"Найдена ссылка Instagram в родительском элементе: {parent_href}")
                                        return parent_href
                            except:
                                break
                except:
                    continue
                    
            # Ищем в исходном коде страницы
            try:
                page_content = await self.page.content()
                # Ищем паттерны с Instagram ссылками
                js_patterns = [
                    r'instagram["\']?\s*:\s*["\']([^"\']+)["\']',
                    r'instagram\.com/([^"\')\s/]+)',
                    r'https://(?:www\.)?instagram\.com/([^"\')\s/]+)',
                    r'window\.open\(["\']([^"\']*instagram[^"\']*)["\']',
                    r'href\s*=\s*["\']([^"\']*instagram[^"\']*)["\']'
                ]
                
                for pattern in js_patterns:
                    matches = re.findall(pattern, page_content, re.IGNORECASE)
                    for match in matches:
                        if match and 'instagram' not in match:
                            # Если найден только username, создаем полную ссылку
                            match = f"https://instagram.com/{match}"
                        elif match and 'instagram' in match and not match.startswith('http'):
                            match = f"https://{match}"
                        
                        if match and 'instagram.com' in match:
                            logger.info(f"Найдена ссылка Instagram в исходном коде: {match}")
                            return match
            except:
                pass
                
        except Exception as e:
            logger.error(f"Ошибка при поиске Instagram: {e}")
            
        return None
        
    async def save_to_excel(self, filename: str):
        """Сохранение результатов в Excel"""
        try:
            if not self.results:
                logger.warning("Нет данных для сохранения")
                return
                
            df = pd.DataFrame(self.results)
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Организации', index=False)
                
                worksheet = writer.sheets['Организации']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                    
            logger.info(f"Данные сохранены в файл: {filename}")
            logger.info(f"Всего записей: {len(self.results)}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в Excel: {e}")
            
    async def parse_category(self, category: str):
        """Парсинг одной категории"""
        try:
            logger.info(f"Начинаем парсинг категории: {category}")
            
            # Выполняем поиск
            if not await self.open_2gis_and_search(category):
                return
                
            # Получаем ссылки на все организации
            business_urls = await self.get_business_links_pagination_fixed()
            
            if not business_urls:
                logger.warning(f"Не найдено организаций для категории '{category}'")
                return
                
            logger.info(f"Будем обрабатывать {len(business_urls)} организаций")
            
            # Обрабатываем каждую организацию
            processed = 0
            for i, url in enumerate(business_urls, 1):
                try:
                    logger.info(f"Обрабатываем организацию {i}/{len(business_urls)}")
                    
                    business_info = await self.extract_business_info(url, category)
                    if business_info:
                        self.results.append(business_info)
                        processed += 1
                        logger.info(f"Добавлена информация о: {business_info['Название']}")
                    
                    await self.random_delay(2, 4)
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке организации {i}: {e}")
                    continue
                    
            logger.info(f"Завершен парсинг категории '{category}'. Собрано {processed} записей")
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге категории '{category}': {e}")
            
    async def run(self, categories: List[str]):
        """Основной метод запуска парсера"""
        try:
            await self.setup_browser()
            
            for category in categories:
                await self.parse_category(category)
                await self.random_delay(3, 5)
                
            # Сохранение результатов
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"2gis_results_{self.city}_{timestamp}.xlsx"
            await self.save_to_excel(filename)
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise
            
        finally:
            if self.browser:
                try:
                    await self.browser.close()
                    logger.info("Браузер закрыт")
                except:
                    pass
            if hasattr(self, 'playwright'):
                try:
                    await self.playwright.stop()
                except:
                    pass

def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Улучшенный парсер 2ГИС с пагинацией')
    parser.add_argument('--city', '-c', default='Астана', help='Город для поиска')
    parser.add_argument('--categories', '-cat', nargs='+', 
                       default=['кофейни'],
                       help='Список категорий для парсинга')
    parser.add_argument('--max-items', '-m', type=int, default=100,
                       help='Максимальное количество организаций на категорию')
    parser.add_argument('--config', help='Путь к JSON файлу с конфигурацией')
    
    args = parser.parse_args()
    
    if args.config:
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                config = json.load(f)
                args.city = config.get('city', args.city)
                args.categories = config.get('categories', args.categories)
                args.max_items = config.get('max_items', args.max_items)
        except Exception as e:
            logger.error(f"Ошибка при загрузке конфигурации: {e}")
    
    parser_instance = GISParser(city=args.city, max_items_per_category=args.max_items)
    
    try:
        asyncio.run(parser_instance.run(args.categories))
        logger.info("Парсинг завершен успешно!")
    except Exception as e:
        logger.error(f"Парсинг завершен с ошибкой: {e}")

if __name__ == "__main__":
    main()