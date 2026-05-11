# etl_core/management/commands/run_etl.py
import pandas as pd
import logging
from datetime import datetime
from django.core.management.base import BaseCommand
from etl_core.models import WebLog

# Настраиваем логгер с записью в файл
logger = logging.getLogger('etl')
logger.setLevel(logging.INFO)

# Создаем обработчик для записи в файл
file_handler = logging.FileHandler('etl_pipeline.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Добавляем вывод в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class Command(BaseCommand):
    help = 'Запускает ETL-пайплайн для загрузки и агрегации логов веб-сервера.'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Путь к CSV-файлу с данными')

    def handle(self, *args, **options):
        csv_file_path = options['csv_file']
        logger.info(f'========== ЗАПУСК ETL-ПАЙПЛАЙНА ==========')
        logger.info(f'Источник данных: {csv_file_path}')
        self.stdout.write(self.style.SUCCESS(f'--- Запуск ETL-пайплайна из {csv_file_path} ---'))

        try:
            # === ЭТАП 1: EXTRACT ===
            logger.info('ЭТАП 1: EXTRACT - начало извлечения данных')
            raw_df = self.extract(csv_file_path)

            # === ЭТАП 2: TRANSFORM ===
            if not raw_df.empty:
                logger.info(f'ЭТАП 2: TRANSFORM - начало очистки (строк: {len(raw_df)})')
                clean_df = self.transform(raw_df)
                logger.info(f'ЭТАП 2: TRANSFORM - завершен (чистых строк: {len(clean_df)})')
            else:
                logger.warning('Данные не извлечены. Пайплайн остановлен.')
                return

            # === ЭТАП 2.5: АГРЕГАЦИЯ ===
            if not clean_df.empty:
                logger.info('ЭТАП 2.5: АГРЕГАЦИЯ - вычисление статистики')
                aggregated_df = self.aggregate(clean_df)
                logger.info(f'ЭТАП 2.5: АГРЕГАЦИЯ - создано {len(aggregated_df)} агрегированных записей')
            else:
                logger.warning('Нет данных для агрегации.')
                return

            # === ЭТАП 3: LOAD ===
            logger.info(f'ЭТАП 3: LOAD - загрузка в БД')
            self.load(clean_df, aggregated_df)

            logger.info('========== ETL-ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН ==========')
            self.stdout.write(self.style.SUCCESS('ETL-пайплайн успешно завершен.'))

        except Exception as e:
            logger.error(f'КРИТИЧЕСКАЯ ОШИБКА: {e}', exc_info=True)
            self.stdout.write(self.style.ERROR(f'Ошибка: {e}'))

    # --- EXTRACT ---
    def extract(self, file_path):
        """Извлекает данные из CSV."""
        try:
            df = pd.read_csv(file_path)
            logger.info(f'EXTRACT: Успешно извлечено {len(df)} строк из {file_path}')
            return df
        except FileNotFoundError:
            logger.error(f'EXTRACT: Файл не найден: {file_path}')
            return pd.DataFrame()
        except Exception as e:
            logger.error(f'EXTRACT: Ошибка чтения файла: {e}')
            return pd.DataFrame()

    # --- TRANSFORM ---
    def transform(self, df):
        """Очищает, преобразует и валидирует данные."""
        initial_rows = len(df)
        logger.info(f'TRANSFORM: Начало обработки {initial_rows} строк')

        # 1. Переименовываем колонки
        df = df.rename(columns={'ip': 'ip_address', 'status': 'status_code'})
        logger.info('TRANSFORM: Колонки переименованы')

        # 2. Удаляем дубликаты
        before_dedup = len(df)
        df = df.drop_duplicates()
        logger.info(f'TRANSFORM: Удалено дубликатов: {before_dedup - len(df)}')

        # 3. Очищаем response_time
        before_resp = df['response_time'].isna().sum()
        df['response_time'] = pd.to_numeric(df['response_time'], errors='coerce')
        after_resp = df['response_time'].isna().sum()
        logger.info(f'TRANSFORM: Некорректных значений response_time: {after_resp - before_resp}')

        # 4. Конвертируем timestamp
        before_date = df['timestamp'].isna().sum()
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        after_date = df['timestamp'].isna().sum()
        logger.info(f'TRANSFORM: Некорректных дат: {after_date - before_date}')

        # 5. Разбираем строку запроса
        def parse_request(request_str):
            try:
                parts = str(request_str).split(' ')
                return parts[0], ' '.join(parts[1:]) if len(parts) > 1 else '/'
            except (AttributeError, IndexError):
                return None, None

        request_data = df['request'].apply(parse_request)
        df['http_method'] = request_data.apply(lambda x: x[0])
        df['url_path'] = request_data.apply(lambda x: x[1])
        df = df.drop(columns=['request'])
        logger.info('TRANSFORM: Запросы разобраны на метод и путь')

        # 6. Удаляем строки с критическими пропусками
        before_dropna = len(df)
        df = df.dropna(subset=['timestamp', 'status_code', 'http_method', 'ip_address'])
        logger.info(f'TRANSFORM: Удалено строк с пропусками: {before_dropna - len(df)}')

        # 7. Валидация IP-адреса
        before_ip = len(df)
        df = df[df['ip_address'].astype(str).str.contains('.', na=False)]
        logger.info(f'TRANSFORM: Удалено некорректных IP: {before_ip - len(df)}')

        # 8. Приведение status_code к целому числу
        before_status = len(df)
        df['status_code'] = pd.to_numeric(df['status_code'], errors='coerce')
        df = df.dropna(subset=['status_code'])
        df['status_code'] = df['status_code'].astype(int)
        logger.info(f'TRANSFORM: Удалено некорректных status_code: {before_status - len(df)}')

        processed_rows = len(df)
        dropped_rows = initial_rows - processed_rows
        logger.info(f'TRANSFORM: ИТОГО - удалено грязных строк: {dropped_rows}, чистых: {processed_rows}')

        return df

    # --- АГРЕГАЦИЯ (НОВЫЙ ЭТАП) ---
    def aggregate(self, df):
        """
        Агрегирует данные: вычисляет статистику по HTTP методам и статусам.
        Создает сводную таблицу для аналитики.
        """
        logger.info('АГРЕГАЦИЯ: Вычисление статистики...')

        # Агрегация 1: Количество запросов по HTTP методам
        method_stats = df.groupby('http_method').agg(
            request_count=('http_method', 'count'),
            avg_response_time=('response_time', 'mean'),
            error_rate=('status_code', lambda x: (x >= 400).sum() / len(x) * 100)
        ).reset_index()
        method_stats.columns = ['http_method', 'request_count', 'avg_response_time_ms', 'error_rate_percent']
        logger.info(f'АГРЕГАЦИЯ: Статистика по методам:\n{method_stats.to_string()}')

        # Агрегация 2: Количество запросов по часам
        df['hour'] = df['timestamp'].dt.hour
        hourly_stats = df.groupby('hour').agg(
            total_requests=('hour', 'count'),
            avg_response_time=('response_time', 'mean'),
            unique_ips=('ip_address', 'nunique')
        ).reset_index()
        hourly_stats.columns = ['hour', 'total_requests', 'avg_response_time_ms', 'unique_visitors']
        logger.info(f'АГРЕГАЦИЯ: Почасовая статистика:\n{hourly_stats.to_string()}')

        # Агрегация 3: Топ страниц по посещаемости
        page_stats = df.groupby('url_path').agg(
            visits=('url_path', 'count'),
            avg_response_time=('response_time', 'mean')
        ).reset_index().sort_values('visits', ascending=False)
        page_stats.columns = ['url_path', 'visit_count', 'avg_response_time_ms']
        logger.info(f'АГРЕГАЦИЯ: Топ страниц:\n{page_stats.to_string()}')

        # Сохраняем агрегации в CSV для дашборда
        method_stats.to_csv('aggregation_method_stats.csv', index=False)
        hourly_stats.to_csv('aggregation_hourly_stats.csv', index=False)
        page_stats.to_csv('aggregation_page_stats.csv', index=False)
        logger.info('АГРЕГАЦИЯ: Результаты сохранены в CSV-файлы')

        return {
            'method_stats': method_stats,
            'hourly_stats': hourly_stats,
            'page_stats': page_stats
        }

    # --- LOAD (ИДЕМПОТЕНТНАЯ ВЕРСИЯ) ---
    def load(self, df, aggregated_data=None):
        """Загружает чистые и агрегированные данные в БД."""
        logger.info('LOAD: Начало загрузки данных...')

        # ИДЕМПОТЕНТНОСТЬ: очищаем таблицу перед загрузкой
        initial_count = WebLog.objects.count()
        logger.info(f'LOAD: Текущее количество записей в БД: {initial_count}')

        deleted_count, _ = WebLog.objects.all().delete()
        logger.info(f'LOAD: Удалено {deleted_count} старых записей (идемпотентность)')

        # Создаем список объектов для загрузки
        logs_to_create = []
        for _, row in df.iterrows():
            logs_to_create.append(
                WebLog(
                    ip_address=row['ip_address'],
                    timestamp=row['timestamp'].to_pydatetime(),
                    http_method=row['http_method'],
                    url_path=row['url_path'],
                    status_code=row['status_code'],
                    response_time=row['response_time'] if pd.notna(row['response_time']) else 0.0,
                )
            )

        # Массовая вставка в БД
        WebLog.objects.bulk_create(logs_to_create)
        logger.info(f'LOAD: Успешно загружено {len(logs_to_create)} записей в таблицу WebLog')