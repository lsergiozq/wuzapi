package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type RedisQueue struct {
	client *redis.Client
	key    string
}

// Inicializa a fila no Redis
func NewRedisQueue(redisAddr string, queueKey string) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	return &RedisQueue{
		client: client,
		key:    queueKey,
	}
}

// Adiciona uma mensagem na fila com prioridade (quanto menor o score, maior a prioridade)
func (q *RedisQueue) Enqueue(message string, priority int) error {
	return q.client.ZAdd(ctx, q.key, redis.Z{
		Score:  float64(priority),
		Member: message,
	}).Err()
}

// Consome a mensagem de maior prioridade (menor score)
func (q *RedisQueue) Dequeue() (string, error) {
	// Pega a mensagem com maior prioridade (score mais baixo)
	msgs, err := q.client.ZRangeWithScores(ctx, q.key, 0, 0).Result()
	if err != nil || len(msgs) == 0 {
		return "", err
	}

	// Remove a mensagem da fila após leitura
	_, err = q.client.ZRem(ctx, q.key, msgs[0].Member).Result()
	if err != nil {
		return "", err
	}

	return msgs[0].Member.(string), nil
}
