PowerShell.exe -Command "& workon webir ; celery -A crawler.tasks worker -l warn --concurrency 50"
PAUSE