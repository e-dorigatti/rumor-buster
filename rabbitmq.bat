PowerShell.exe -Command "& docker run -d -p 5672:5672 -p 15672:15672 -v "$pwd\rabbitmqdata:/var/lib/rabbitmq/mnesia/rabbit@rumor-rabbit" --hostname rumor-rabbit rabbitmq:latest"
PAUSE