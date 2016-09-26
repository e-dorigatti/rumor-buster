PowerShell.exe -Command "& workon webir ; celery -A crawler.tasks worker -l info"
PAUSE