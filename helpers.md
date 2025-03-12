# Comandos úteis 

## Para atualizar o whatsmeow

```
go get go.mau.fi/whatsmeow@latest
```

## Para reiniciar o rabbitmq

```
sudo docker restart rabbitmq
```

## Para recriar o rabbitmq no docker
### latest RabbitMQ 4.0.x

```
sudo docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management
```

## Docker comandos
### Visualizar imagens

```
sudo docker ps
```

### Deletar uma imagem

```
sudo docker stop containerid
sudo docker rm containerid
```