from django.db import models


class WebLog(models.Model):
    """Модель для хранения обработанных логов веб-сервера."""
    # Поля, которые мы будем извлекать и преобразовывать
    ip_address = models.GenericIPAddressField(verbose_name="IP адрес")
    timestamp = models.DateTimeField(verbose_name="Дата и время запроса")
    http_method = models.CharField(max_length=10, verbose_name="HTTP метод")  # GET, POST ...
    url_path = models.CharField(max_length=2048, verbose_name="Путь запроса")
    status_code = models.PositiveSmallIntegerField(verbose_name="Код ответа")
    response_time = models.FloatField(verbose_name="Время ответа (мс)",
                                      help_text="Время обработки запроса в миллисекундах")

    # Поле для фиксации времени загрузки данных
    loaded_at = models.DateTimeField(auto_now_add=True, verbose_name="Время загрузки в БД")

    class Meta:
        # Данные будут загружаться пачками, поэтому важен индекс по времени запроса для быстрого анализа
        indexes = [
            models.Index(fields=['timestamp']),
            models.Index(fields=['status_code']),
        ]
        verbose_name = "Запись лога веб-сервера"
        verbose_name_plural = "Записи логов веб-сервера"

    def __str__(self):
        return f"{self.timestamp} - {self.http_method} {self.url_path} -> {self.status_code}"