package api

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/solomq/config"
	"github.com/solomq/internal/broker"
	"github.com/solomq/internal/models"
)

type API struct {
	broker *broker.Broker
}

func NewAPI(b *broker.Broker) *API {
	return &API{broker: b}
}

func (a *API) AdminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Admin-Token")
		if token == "" {
			token = c.Query("token")
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(config.AppConfig.AdminToken)) != 1 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	}
}

func (a *API) CreateTopic(c *gin.Context) {
	var config models.TopicConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = 1048576
	}

	topic, err := a.broker.CreateTopic(config)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, topic)
}

func (a *API) GetTopics(c *gin.Context) {
	topics := a.broker.GetAllTopics()
	c.JSON(http.StatusOK, topics)
}

func (a *API) GetTopic(c *gin.Context) {
	name := c.Param("name")
	topic, exists := a.broker.GetTopic(name)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "topic not found"})
		return
	}
	c.JSON(http.StatusOK, topic)
}

func (a *API) Publish(c *gin.Context) {
	topicName := c.Param("topic")

	var req models.CreateMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Payload == nil {
		req.Payload = []byte(c.PostForm("payload"))
	}

	msgID, err := a.broker.Publish(topicName, &req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"id": msgID, "status": "published"})
}

func (a *API) Consume(c *gin.Context) {
	topicName := c.Param("topic")
	groupName := c.Query("group")
	instanceID := c.Query("instance")
	maxStr := c.Query("max")
	timeoutStr := c.Query("timeout")

	if groupName == "" || instanceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group and instance are required"})
		return
	}

	max := 1
	if maxStr != "" {
		if m, err := strconv.Atoi(maxStr); err == nil && m > 0 {
			max = m
		}
	}

	timeout := config.AppConfig.LongPollTimeout
	if timeoutStr != "" {
		if t, err := strconv.Atoi(timeoutStr); err == nil && t > 0 {
			timeout = time.Duration(t) * time.Second
		}
	}

	messages, err := a.broker.Consume(topicName, groupName, instanceID, max, timeout)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"messages": messages,
		"count":    len(messages),
	})
}

func (a *API) Ack(c *gin.Context) {
	topicName := c.Param("topic")
	groupName := c.Query("group")
	instanceID := c.Query("instance")
	msgID := c.Param("id")

	if groupName == "" || instanceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group and instance are required"})
		return
	}

	if err := a.broker.Ack(topicName, groupName, instanceID, msgID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "acked"})
}

func (a *API) Nack(c *gin.Context) {
	topicName := c.Param("topic")
	groupName := c.Query("group")
	instanceID := c.Query("instance")
	msgID := c.Param("id")

	if groupName == "" || instanceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group and instance are required"})
		return
	}

	if err := a.broker.Nack(topicName, groupName, instanceID, msgID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "nacked"})
}

func (a *API) Heartbeat(c *gin.Context) {
	topicName := c.Param("topic")
	groupName := c.Query("group")
	instanceID := c.Query("instance")

	if groupName == "" || instanceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group and instance are required"})
		return
	}

	if err := a.broker.Heartbeat(topicName, groupName, instanceID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *API) Replay(c *gin.Context) {
	topicName := c.Param("topic")
	groupName := c.Param("group")

	var offsets map[int]int64
	if err := c.ShouldBindJSON(&offsets); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := a.broker.Replay(topicName, groupName, offsets); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "replayed"})
}

func (a *API) GetConsumerGroups(c *gin.Context) {
	topicName := c.Param("topic")
	groups := a.broker.GetConsumerGroups(topicName)
	c.JSON(http.StatusOK, groups)
}

func (a *API) GetDLQ(c *gin.Context) {
	topicName := c.Param("topic")
	messages := a.broker.GetDLQMessages(topicName)
	c.JSON(http.StatusOK, messages)
}

func (a *API) ResendDLQ(c *gin.Context) {
	topicName := c.Param("topic")
	msgID := c.Param("id")

	if err := a.broker.ResendDLQMessage(topicName, msgID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "resent"})
}

func (a *API) GetMetrics(c *gin.Context) {
	metrics := a.broker.GetMetrics()
	topicMetrics := metrics.GetAllTopicMetrics()
	groupMetrics := metrics.GetAllConsumerGroupMetrics()

	c.JSON(http.StatusOK, gin.H{
		"topics":         topicMetrics,
		"consumerGroups": groupMetrics,
	})
}

func (a *API) GetTopicMetrics(c *gin.Context) {
	topicName := c.Param("topic")
	metrics := a.broker.GetMetrics()
	tm, exists := metrics.GetTopicMetrics(topicName)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "topic not found"})
		return
	}
	c.JSON(http.StatusOK, tm)
}

func (a *API) ModifyOffset(c *gin.Context) {
	topicName := c.Param("topic")
	groupName := c.Param("group")

	var req struct {
		Partition  int   `json:"partition"`
		Offset     int64 `json:"offset"`
		Confirmed  bool  `json:"confirmed"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !req.Confirmed {
		c.JSON(http.StatusOK, gin.H{
			"status": "confirmation_required",
			"message": "Please confirm this operation by setting 'confirmed: true'. This will reset the consumer offset and may cause messages to be reprocessed.",
		})
		return
	}

	offsets := map[int]int64{req.Partition: req.Offset}
	if err := a.broker.Replay(topicName, groupName, offsets); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "offset_modified"})
}

func (a *API) SetupRoutes(r *gin.Engine) {
	api := r.Group("/api/v1")
	{
		topics := api.Group("/topics")
		{
			topics.POST("", a.AdminAuthMiddleware(), a.CreateTopic)
			topics.GET("", a.GetTopics)
			topics.GET("/:name", a.GetTopic)

			topics.POST("/:topic/publish", a.Publish)
			topics.POST("/:topic/consume", a.Consume)
			topics.POST("/:topic/ack/:id", a.Ack)
			topics.POST("/:topic/nack/:id", a.Nack)
			topics.POST("/:topic/heartbeat", a.Heartbeat)

			topics.GET("/:topic/groups", a.AdminAuthMiddleware(), a.GetConsumerGroups)
			topics.POST("/:topic/groups/:group/replay", a.AdminAuthMiddleware(), a.Replay)
			topics.POST("/:topic/groups/:group/offset", a.AdminAuthMiddleware(), a.ModifyOffset)

			topics.GET("/:topic/dlq", a.AdminAuthMiddleware(), a.GetDLQ)
			topics.POST("/:topic/dlq/:id/resend", a.AdminAuthMiddleware(), a.ResendDLQ)

			topics.GET("/:topic/metrics", a.GetTopicMetrics)
		}

		api.GET("/metrics", a.GetMetrics)
	}

	admin := r.Group("/admin")
	admin.Use(a.AdminAuthMiddleware())
	{
		admin.GET("", a.Dashboard)
		admin.GET("/topic/:name", a.TopicDetail)
		admin.POST("/topic/:name/dlq/:id/resend", a.ResendDLQWeb)
		admin.POST("/topic/:name/groups/:group/offset", a.ModifyOffsetWeb)
	}

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.Static("/static", "./static")
	r.LoadHTMLGlob("templates/*.html")
}
