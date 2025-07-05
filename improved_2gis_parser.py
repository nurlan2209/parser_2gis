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
        
        # Добавляем хранилище для отслеживания уникальных компаний
        self.processed_companies = set()  # Для быстрой проверки
        self.company_details = {}  # Для хранения детальной информации
        
    def normalize_company_name(self, name: str) -> str:
        """Нормализация названия компании для сравнения"""
        if not name or name == 'Не указано':
            return ''
            
        # Приводим к нижнему регистру
        normalized = name.lower().strip()
        
        # Убираем лишние пробелы и символы
        normalized = re.sub(r'\s+', ' ', normalized)
        normalized = re.sub(r'[^\w\s]', '', normalized, flags=re.UNICODE)
        
        # Убираем типичные суффиксы филиалов
        suffixes_to_remove = [
            r'\s*филиал\s*\d*',
            r'\s*отделение\s*\d*', 
            r'\s*магазин\s*\d*',
            r'\s*точка\s*\d*',
            r'\s*№\s*\d+',
            r'\s*\d+\s*$',  # Цифры в конце
            r'\s*mall\s*',
            r'\s*тц\s*',
            r'\s*тдц\s*',
            r'\s*центр\s*',
            r'\s*фудкорт\s*',
            r'\s*торговый\s*центр\s*',
        ]
        
        for suffix in suffixes_to_remove:
            normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)
        
        # Убираем названия торговых центров в скобках или после запятой
        normalized = re.sub(r'\([^)]*\)', '', normalized)  # Убираем все в скобках
        normalized = re.sub(r',.*$', '', normalized)  # Убираем все после запятой
        
        # Финальная очистка
        normalized = normalized.strip()
        
        return normalized
    
    def is_company_already_processed(self, name: str, address: str = None) -> bool:
        """Проверка, была ли компания уже обработана"""
        normalized_name = self.normalize_company_name(name)
        
        if not normalized_name:
            return False
            
        # Простая проверка по названию
        if normalized_name in self.processed_companies:
            logger.info(f"🔄 Компания '{name}' уже обработана (дубликат)")
            return True
            
        # Дополнительная проверка по похожим названиям
        for existing_name in self.processed_companies:
            # Проверяем схожесть названий (например, если одно содержится в другом)
            if len(normalized_name) > 3 and len(existing_name) > 3:
                if normalized_name in existing_name or existing_name in normalized_name:
                    # Дополнительно проверяем адрес если есть
                    if address and existing_name in self.company_details:
                        existing_address = self.company_details[existing_name].get('address', '')
                        if existing_address and address:
                            # Если адреса разные, возможно это разные компании
                            if not self.addresses_similar(address, existing_address):
                                continue
                    
                    logger.info(f"🔄 Найдена похожая компания: '{name}' ≈ '{existing_name}' (пропускаем)")
                    return True
        
        return False
    
    def addresses_similar(self, addr1: str, addr2: str) -> bool:
        """Проверка схожести адресов"""
        if not addr1 or not addr2:
            return False
            
        # Нормализуем адреса
        addr1_norm = re.sub(r'[^\w\s]', ' ', addr1.lower())
        addr2_norm = re.sub(r'[^\w\s]', ' ', addr2.lower())
        
        # Убираем номера домов/квартир для сравнения района
        addr1_base = re.sub(r'\d+', '', addr1_norm).strip()
        addr2_base = re.sub(r'\d+', '', addr2_norm).strip()
        
        # Если базовые части адресов совпадают более чем на 70%
        common_words = set(addr1_base.split()) & set(addr2_base.split())
        total_words = set(addr1_base.split()) | set(addr2_base.split())
        
        if total_words and len(common_words) / len(total_words) > 0.7:
            return True
            
        return False
    
    def add_company_to_processed(self, name: str, business_info: Dict):
        """Добавление компании в список обработанных"""
        normalized_name = self.normalize_company_name(name)
        if normalized_name:
            self.processed_companies.add(normalized_name)
            self.company_details[normalized_name] = {
                'original_name': name,
                'address': business_info.get('Адрес', ''),
                'category': business_info.get('Категория', ''),
                'phone': business_info.get('Телефон', ''),
                'website': business_info.get('Сайт', '')
            }
            logger.debug(f"✅ Добавлена в обработанные: '{normalized_name}'")
        
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
        """Улучшенное извлечение информации с проверкой дубликатов"""
        try:
            logger.info(f"Переходим на страницу: {url}")
            
            # Переходим на страницу
            await self.page.goto(url, wait_until="domcontentloaded", timeout=20000)
            
            # Ждем загрузки динамического контента
            await self.wait_for_dynamic_content()
            
            # Сначала извлекаем название для проверки дубликатов
            name = None
            try:
                name = await self.extract_text_by_selectors([
                    'h1', 'h2', '[class*="title"]', '[class*="name"]', '[class*="header"]'
                ])
            except Exception as e:
                logger.debug(f"Ошибка при извлечении названия: {e}")
            
            # Проверяем, не обрабатывали ли мы уже эту компанию
            if name and self.is_company_already_processed(name):
                logger.info(f"⏭️ Пропускаем дубликат: {name}")
                return None
            
            # Если компания новая, продолжаем извлечение остальной информации
            address = None
            phone = None
            website = None
            whatsapp = None
            instagram = None
            
            try:
                address = await self.extract_address()
            except Exception as e:
                logger.debug(f"Ошибка при извлечении адреса: {e}")
            
            # Дополнительная проверка по адресу (если название слишком общее)
            if name and address and self.is_company_already_processed(name, address):
                logger.info(f"⏭️ Пропускаем дубликат по адресу: {name} - {address}")
                return None
            
            try:
                phone = await self.extract_phone()
            except Exception as e:
                logger.debug(f"Ошибка при извлечении телефона: {e}")
            
            try:
                website = await self.extract_website()  # Используем исправленную версию
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
            
            # Добавляем компанию в список обработанных
            if name:
                self.add_company_to_processed(name, result)
            
            logger.info(f"✅ Собрана информация: {result['Название']}, {result['Адрес']}, {result['Телефон']}")
            if website and website != 'Не указано':
                logger.info(f"🌐 Сайт: {result['Сайт']}")
            if whatsapp and whatsapp != 'Не указано':
                logger.info(f"📱 WhatsApp: {result['WhatsApp']}")
            if instagram and instagram != 'Не указано':
                logger.info(f"📸 Instagram: {result['Instagram']}")
            
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
        """Улучшенное декодирование 2ГИС ссылок для сайтов"""
        try:
            import base64
            import urllib.parse
            
            if 'link.2gis.com' not in link:
                return None
                
            logger.debug(f"Декодируем 2ГИС ссылку: {link}")
            
            # Извлекаем закодированную часть
            parts = link.split('/')
            if len(parts) < 2:
                return None
                
            encoded_part = parts[-1]
            
            # Убираем query параметры
            if '?' in encoded_part:
                encoded_part = encoded_part.split('?')[0]
            if '#' in encoded_part:
                encoded_part = encoded_part.split('#')[0]
                
            try:
                # Пробуем декодировать с разными вариантами padding
                for padding in ['', '=', '==', '===']:
                    try:
                        padded_data = encoded_part + padding
                        decoded_bytes = base64.b64decode(padded_data)
                        decoded_string = decoded_bytes.decode('utf-8')
                        
                        logger.debug(f"Декодированная строка: {decoded_string[:200]}...")
                        
                        # Ищем URL в декодированной строке
                        url_patterns = [
                            r'https?://([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:kz|com|ru|org|net|biz|cafe|coffee)(?:/[^\s]*)?)',
                            r'http://([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:kz|com|ru|org|net|biz|cafe|coffee)(?:/[^\s]*)?)',
                            r'([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:kz|com|ru|org|net|biz|cafe|coffee)(?:/[^\s]*)?)'
                        ]
                        
                        for pattern in url_patterns:
                            matches = re.findall(pattern, decoded_string, re.IGNORECASE)
                            for match in matches:
                                domain = match if isinstance(match, str) else match[0]
                                
                                # Исключаем служебные домены
                                if not any(bad in domain.lower() for bad in ['2gis', 'sberbank', 'yandex', 'google']):
                                    if len(domain) > 6:
                                        # Проверяем, есть ли уже протокол
                                        if domain.startswith('http'):
                                            result = domain
                                        else:
                                            result = f"https://{domain}"
                                        logger.info(f"Декодирован сайт: {result}")
                                        return result
                        
                        break  # Если декодирование прошло успешно, выходим
                    except:
                        continue
                        
            except Exception as e:
                logger.debug(f"Ошибка декодирования: {e}")
                
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка при декодировании ссылки: {e}")
            return None

    async def extract_website(self) -> Optional[str]:
        """Исправленное извлечение сайта по SVG иконке глобуса и специфичным классам"""
        try:
            logger.info("Ищем сайт по SVG иконке глобуса...")

            # Ищем SVG с иконкой глобуса (земли)
            svg_selectors = [
                'svg[fill="#028eff"]',  # Конкретный цвет из примера
                'svg',  # Все SVG элементы
                'div._1iftozu svg'  # SVG внутри div с классом _1iftozu
            ]
            
            for svg_selector in svg_selectors:
                try:
                    svg_elements = await self.page.query_selector_all(svg_selector)
                    
                    for svg in svg_elements:
                        try:
                            # Проверяем, что это иконка глобуса по path
                            path_element = await svg.query_selector('path')
                            if path_element:
                                path_d = await path_element.get_attribute('d')
                                # Проверяем характерные части path для иконки глобуса
                                if path_d and any(pattern in path_d for pattern in ['M12 4a8 8', 'a8 8 0', 'A6 6 0']):
                                    logger.debug("Найдена SVG иконка глобуса")
                                    
                                    # Ищем родительский контейнер с сайтом
                                    current_element = svg
                                    for level in range(5):  # Проверяем до 5 уровней вверх
                                        try:
                                            parent = await current_element.query_selector('xpath=..')
                                            if not parent:
                                                break
                                                
                                            # Ищем div с классом _49kxlr рядом или внутри
                                            website_containers = await parent.query_selector_all('div._49kxlr, ._49kxlr')
                                            
                                            for container in website_containers:
                                                # Ищем ссылку внутри контейнера
                                                links = await container.query_selector_all('a')
                                                for link in links:
                                                    # Проверяем href
                                                    href = await link.get_attribute('href')
                                                    if href and 'link.2gis.com' in href:
                                                        # Декодируем 2ГИС ссылку
                                                        decoded_site = await self.decode_2gis_website_link(href)
                                                        if decoded_site:
                                                            logger.info(f"Декодирован сайт: {decoded_site}")
                                                            return decoded_site
                                                    
                                                    # Проверяем текст ссылки (должен быть доменом)
                                                    link_text = await link.text_content()
                                                    if link_text:
                                                        link_text = link_text.strip()
                                                        logger.debug(f"Найден текст ссылки: {link_text}")
                                                        
                                                        # Проверяем, что это домен (включая поддомены)
                                                        if self.is_valid_domain(link_text):
                                                            # НЕ убираем поддомены - берем как есть
                                                            if not link_text.startswith('http'):
                                                                result = f"https://{link_text}"
                                                            else:
                                                                result = link_text
                                                            logger.info(f"Найден сайт: {result}")
                                                            return result
                                            
                                            current_element = parent
                                        except:
                                            break
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"Ошибка при поиске SVG {svg_selector}: {e}")
                    continue

            # Если не нашли по SVG, пробуем прямой поиск по классам
            logger.info("Поиск по SVG не дал результатов, ищем напрямую по классам...")
            
            try:
                # Ищем все элементы с классом _49kxlr
                website_elements = await self.page.query_selector_all('._49kxlr, div._49kxlr')
                logger.debug(f"Найдено элементов с классом _49kxlr: {len(website_elements)}")
                
                for element in website_elements:
                    try:
                        # Ищем ссылки внутри
                        links = await element.query_selector_all('a')
                        for link in links:
                            # Проверяем href на 2gis ссылку
                            href = await link.get_attribute('href')
                            if href and 'link.2gis.com' in href:
                                decoded_site = await self.decode_2gis_website_link(href)
                                if decoded_site:
                                    logger.info(f"Декодирован сайт из _49kxlr: {decoded_site}")
                                    return decoded_site
                            
                            # Проверяем текст ссылки
                            link_text = await link.text_content()
                            if link_text:
                                link_text = link_text.strip()
                                
                                if self.is_valid_domain(link_text):
                                    if not link_text.startswith('http'):
                                        result = f"https://{link_text}"
                                    else:
                                        result = link_text
                                    logger.info(f"Найден сайт в _49kxlr: {result}")
                                    return result
                    except Exception as e:
                        logger.debug(f"Ошибка при обработке элемента _49kxlr: {e}")
                        continue
            except Exception as e:
                logger.debug(f"Ошибка при поиске по классу _49kxlr: {e}")

            logger.info("Сайт не найден")
            return None
                    
        except Exception as e:
            logger.error(f"Критическая ошибка при поиске сайта: {e}")
            return None

    def is_valid_domain(self, domain: str) -> bool:
        """Проверка, что строка является валидным доменом (включая поддомены)"""
        try:
            if not domain:
                return False
                
            # Убираем протокол если есть
            domain = domain.replace('https://', '').replace('http://', '')
            
            # Убираем путь если есть
            domain = domain.split('/')[0]
            
            # Проверяем базовый паттерн домена (включая поддомены)
            domain_pattern = r'^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
            
            if re.match(domain_pattern, domain):
                # Дополнительные проверки
                parts = domain.split('.')
                
                # Должно быть минимум 2 части (домен.зона)
                if len(parts) < 2:
                    return False
                    
                # Проверяем зону (последняя часть)
                tld = parts[-1].lower()
                valid_tlds = ['com', 'ru', 'kz', 'org', 'net', 'biz', 'info', 'cafe', 'coffee', 'shop', 'store']
                
                if tld in valid_tlds:
                    # Исключаем служебные домены
                    excluded_domains = ['2gis', 'google', 'yandex', 'facebook', 'vk']
                    if not any(excluded in domain.lower() for excluded in excluded_domains):
                        return True
            
            return False
            
        except:
            return False
        
    async def decode_2gis_link(self, link: str) -> Optional[str]:
        """Улучшенное декодирование ссылок 2ГИС для WhatsApp"""
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
        """Сохранение результатов в Excel с информацией о дедупликации"""
        try:
            if not self.results:
                logger.warning("Нет данных для сохранения")
                return
                
            df = pd.DataFrame(self.results)
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Основные данные
                df.to_excel(writer, sheet_name='Организации', index=False)
                
                # Статистика дедупликации
                stats_data = {
                    'Метрика': [
                        'Всего уникальных компаний',
                        'Всего записей в результате',
                        'Пропущено дубликатов',
                        'Категорий обработано',
                        'Компаний с сайтами',
                        'Компаний с WhatsApp',
                        'Компаний с Instagram'
                    ],
                    'Значение': [
                        len(self.processed_companies),
                        len(self.results),
                        max(0, len(self.processed_companies) - len(self.results)),
                        len(set(result['Категория'] for result in self.results)),
                        len([r for r in self.results if r['Есть сайт'] == 'Да']),
                        len([r for r in self.results if r['WhatsApp'] != 'Не указано']),
                        len([r for r in self.results if r['Instagram'] != 'Не указано'])
                    ]
                }
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Статистика', index=False)
                
                # Форматирование
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
                    
            logger.info(f"📊 Данные сохранены в файл: {filename}")
            logger.info(f"📈 Всего записей: {len(self.results)}")
            logger.info(f"🔄 Уникальных компаний: {len(self.processed_companies)}")
            logger.info(f"⏭️ Пропущено дубликатов: {max(0, len(self.processed_companies) - len(self.results))}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в Excel: {e}")

    def get_deduplication_stats(self) -> Dict:
        """Получение статистики дедупликации"""
        return {
            'unique_companies': len(self.processed_companies),
            'total_records': len(self.results),
            'duplicates_skipped': max(0, len(self.processed_companies) - len(self.results)),
            'categories_processed': len(set(result['Категория'] for result in self.results)),
            'companies_with_websites': len([r for r in self.results if r['Есть сайт'] == 'Да']),
            'companies_with_whatsapp': len([r for r in self.results if r['WhatsApp'] != 'Не указано']),
            'companies_with_instagram': len([r for r in self.results if r['Instagram'] != 'Не указано'])
        }
            
    async def parse_category(self, category: str):
        """Парсинг одной категории с дедупликацией"""
        try:
            logger.info(f"🎯 Начинаем парсинг категории: {category}")
            
            # Выполняем поиск
            if not await self.open_2gis_and_search(category):
                return
                
            # Получаем ссылки на все организации
            business_urls = await self.get_business_links_pagination_fixed()
            
            if not business_urls:
                logger.warning(f"❌ Не найдено организаций для категории '{category}'")
                return
                
            logger.info(f"🔍 Будем обрабатывать {len(business_urls)} организаций")
            
            # Обрабатываем каждую организацию
            processed = 0
            skipped = 0
            
            for i, url in enumerate(business_urls, 1):
                try:
                    logger.info(f"📋 Обрабатываем организацию {i}/{len(business_urls)}")
                    
                    business_info = await self.extract_business_info(url, category)
                    if business_info:
                        self.results.append(business_info)
                        processed += 1
                        logger.info(f"✅ Добавлена информация о: {business_info['Название']}")
                    else:
                        skipped += 1
                        logger.info(f"⏭️ Организация пропущена (дубликат или ошибка)")
                    
                    await self.random_delay(2, 4)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке организации {i}: {e}")
                    skipped += 1
                    continue
                    
            logger.info(f"🎉 Завершен парсинг категории '{category}'")
            logger.info(f"📊 Обработано: {processed}, Пропущено: {skipped}")
            
        except Exception as e:
            logger.error(f"💥 Ошибка при парсинге категории '{category}': {e}")
            
    async def run(self, categories: List[str]):
        """Основной метод запуска парсера с дедупликацией"""
        try:
            logger.info(f"🚀 Запуск парсера для города: {self.city}")
            logger.info(f"📝 Категории: {', '.join(categories)}")
            logger.info(f"🎯 Максимум на категорию: {self.max_items_per_category}")
            
            await self.setup_browser()
            
            for i, category in enumerate(categories, 1):
                logger.info(f"\n{'='*50}")
                logger.info(f"📂 Категория {i}/{len(categories)}: {category}")
                logger.info(f"{'='*50}")
                
                await self.parse_category(category)
                
                # Показываем промежуточную статистику
                stats = self.get_deduplication_stats()
                logger.info(f"📊 Промежуточная статистика:")
                logger.info(f"   • Уникальных компаний: {stats['unique_companies']}")
                logger.info(f"   • Записей в результате: {stats['total_records']}")
                logger.info(f"   • Пропущено дубликатов: {stats['duplicates_skipped']}")
                
                if i < len(categories):  # Пауза между категориями
                    logger.info(f"⏳ Пауза перед следующей категорией...")
                    await self.random_delay(5, 8)
                
            # Сохранение результатов
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"2gis_results_{self.city}_{timestamp}.xlsx"
            await self.save_to_excel(filename)
            
            # Финальная статистика
            final_stats = self.get_deduplication_stats()
            logger.info(f"\n🎯 ФИНАЛЬНАЯ СТАТИСТИКА:")
            logger.info(f"{'='*40}")
            logger.info(f"📈 Всего уникальных компаний: {final_stats['unique_companies']}")
            logger.info(f"📋 Записей в результате: {final_stats['total_records']}")
            logger.info(f"🔄 Пропущено дубликатов: {final_stats['duplicates_skipped']}")
            logger.info(f"📂 Категорий обработано: {final_stats['categories_processed']}")
            logger.info(f"🌐 Компаний с сайтами: {final_stats['companies_with_websites']}")
            logger.info(f"📱 Компаний с WhatsApp: {final_stats['companies_with_whatsapp']}")
            logger.info(f"📸 Компаний с Instagram: {final_stats['companies_with_instagram']}")
            logger.info(f"💾 Файл сохранен: {filename}")
            logger.info(f"{'='*40}")
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            raise
            
        finally:
            if self.browser:
                try:
                    await self.browser.close()
                    logger.info("🔒 Браузер закрыт")
                except:
                    pass
            if hasattr(self, 'playwright'):
                try:
                    await self.playwright.stop()
                except:
                    pass

def main():
    """Главная функция с улучшенной обработкой аргументов"""
    parser = argparse.ArgumentParser(
        description='Улучшенный парсер 2ГИС с дедупликацией и исправленным извлечением сайтов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python improved_2gis_parser.py --city "Астана" --categories "кофейни" "салоны красоты" --max-items 50
  python improved_2gis_parser.py --config config.json
  python improved_2gis_parser.py --categories "стоматологии" "фитнес-центры" "рестораны"
        """
    )
    
    parser.add_argument('--city', '-c', default='Астана', 
                       help='Город для поиска (по умолчанию: Астана)')
    parser.add_argument('--categories', '-cat', nargs='+', 
                       default=['кофейни'],
                       help='Список категорий для парсинга (по умолчанию: кофейни)')
    parser.add_argument('--max-items', '-m', type=int, default=100,
                       help='Максимальное количество организаций на категорию (по умолчанию: 100)')
    parser.add_argument('--config', '-cfg', 
                       help='Путь к JSON файлу с конфигурацией')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Подробный вывод (DEBUG уровень логирования)')
    
    args = parser.parse_args()
    
    # Настройка уровня логирования
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔍 Включен подробный режим логирования")
    
    # Загрузка конфигурации из файла
    if args.config:
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                config = json.load(f)
                args.city = config.get('city', args.city)
                args.categories = config.get('categories', args.categories)
                args.max_items = config.get('max_items', args.max_items)
                logger.info(f"📄 Конфигурация загружена из {args.config}")
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке конфигурации: {e}")
            return
    
    # Валидация аргументов
    if args.max_items <= 0:
        logger.error("❌ Максимальное количество элементов должно быть больше 0")
        return
        
    if not args.categories:
        logger.error("❌ Необходимо указать хотя бы одну категорию")
        return
    
    # Создание и запуск парсера
    logger.info(f"🎯 Инициализация парсера...")
    parser_instance = GISParser(
        city=args.city, 
        max_items_per_category=args.max_items
    )
    
    try:
        logger.info(f"▶️ Запуск парсинга...")
        asyncio.run(parser_instance.run(args.categories))
        logger.info("🎉 Парсинг завершен успешно!")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Парсинг прерван пользователем")
        
    except Exception as e:
        logger.error(f"💥 Парсинг завершен с ошибкой: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())