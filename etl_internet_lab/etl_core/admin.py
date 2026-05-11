# etl_core/admin.py
from django.contrib import admin
from .models import WebLog

@admin.register(WebLog)
class WebLogAdmin(admin.ModelAdmin):
    list_display = ('ip_address', 'timestamp', 'http_method', 'url_path', 'status_code', 'response_time')
    list_filter = ('status_code', 'http_method')
    search_fields = ('ip_address', 'url_path')