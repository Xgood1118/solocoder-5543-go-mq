package api

import (
	"net/http"
	"sort"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (a *API) Dashboard(c *gin.Context) {
	topics := a.broker.GetAllTopics()
	metrics := a.broker.GetMetrics()
	topicMetrics := metrics.GetAllTopicMetrics()
	groupMetrics := metrics.GetAllConsumerGroupMetrics()

	type TopicView struct {
		Name          string
		Partitions    int
		TotalMessages int64
		InFlight      int64
		DLQCount      int64
		TotalLag      int64
		PublishRate   float64
		ConsumeRate   float64
	}

	type GroupView struct {
		Topic     string
		Group     string
		TotalLag  int64
		Instances int
		InFlight  int64
	}

	topicViews := make([]TopicView, 0, len(topics))
	for _, topic := range topics {
		tm, _ := metrics.GetTopicMetrics(topic.Config.Name)
		view := TopicView{
			Name:       topic.Config.Name,
			Partitions: topic.Config.Partitions,
		}
		if tm != nil {
			view.TotalMessages = tm.TotalMessages
			view.InFlight = tm.InFlightCount
			view.DLQCount = tm.DLQCount
			view.TotalLag = tm.TotalLag
			view.PublishRate = tm.PublishRate
			view.ConsumeRate = tm.ConsumeRate
		}
		topicViews = append(topicViews, view)
	}

	groupViews := make([]GroupView, 0, len(groupMetrics))
	for _, gm := range groupMetrics {
		groupViews = append(groupViews, GroupView{
			Topic:     gm.Topic,
			Group:     gm.GroupName,
			TotalLag:  gm.TotalLag,
			Instances: gm.InstanceCount,
			InFlight:  gm.InFlightCount,
		})
	}

	sort.Slice(groupViews, func(i, j int) bool {
		return groupViews[i].TotalLag > groupViews[j].TotalLag
	})

	c.HTML(http.StatusOK, "dashboard.html", gin.H{
		"topics": topicViews,
		"groups": groupViews,
		"token":  c.Query("token"),
	})
}

func (a *API) TopicDetail(c *gin.Context) {
	topicName := c.Param("name")
	topic, exists := a.broker.GetTopic(topicName)
	if !exists {
		c.String(http.StatusNotFound, "Topic not found")
		return
	}

	metrics := a.broker.GetMetrics()
	tm, _ := metrics.GetTopicMetrics(topicName)
	groups := a.broker.GetConsumerGroups(topicName)
	dlq := a.broker.GetDLQMessages(topicName)

	type GroupView struct {
		Name          string
		TotalLag      int64
		PartitionLags map[int]int64
		Instances     int
		InFlight      int64
		Offsets       map[int]int64
	}

	groupViews := make([]GroupView, 0, len(groups))
	for _, cg := range groups {
		gm, _ := metrics.GetConsumerGroupMetrics(topicName, cg.Name)
		view := GroupView{
			Name:    cg.Name,
			Offsets: cg.GetAllOffsets(),
		}
		if gm != nil {
			view.TotalLag = gm.TotalLag
			view.PartitionLags = gm.PartitionLags
			view.Instances = gm.InstanceCount
			view.InFlight = gm.InFlightCount
		}
		groupViews = append(groupViews, view)
	}

	sort.Slice(groupViews, func(i, j int) bool {
		return groupViews[i].TotalLag > groupViews[j].TotalLag
	})

	type DLQView struct {
		ID          string
		Partition   int
		Offset      int64
		RetryCount  int
		CreatedAt   int64
		Payload     string
	}

	dlqViews := make([]DLQView, 0, len(dlq))
	for _, msg := range dlq {
		payload := string(msg.Payload)
		if len(payload) > 200 {
			payload = payload[:200] + "..."
		}
		dlqViews = append(dlqViews, DLQView{
			ID:         msg.ID,
			Partition:  msg.Partition,
			Offset:     msg.Offset,
			RetryCount: msg.RetryCount,
			CreatedAt:  msg.CreatedAt,
			Payload:    payload,
		})
	}

	c.HTML(http.StatusOK, "topic.html", gin.H{
		"topic":       topic,
		"metrics":     tm,
		"groups":      groupViews,
		"dlq":         dlqViews,
		"token":       c.Query("token"),
		"partitions":  topic.Config.Partitions,
	})
}

func (a *API) ResendDLQWeb(c *gin.Context) {
	topicName := c.Param("name")
	msgID := c.Param("id")
	token := c.Query("token")

	if err := a.broker.ResendDLQMessage(topicName, msgID); err != nil {
		c.Redirect(http.StatusFound, "/admin/topic/"+topicName+"?token="+token+"&error="+err.Error())
		return
	}

	c.Redirect(http.StatusFound, "/admin/topic/"+topicName+"?token="+token+"&success=DLQ+message+resent")
}

func (a *API) ModifyOffsetWeb(c *gin.Context) {
	topicName := c.Param("name")
	groupName := c.Param("group")
	token := c.Query("token")

	confirmed := c.PostForm("confirmed") == "true"
	partition, _ := strconv.Atoi(c.PostForm("partition"))
	offset, _ := strconv.ParseInt(c.PostForm("offset"), 10, 64)

	if !confirmed {
		c.Redirect(http.StatusFound, "/admin/topic/"+topicName+"?token="+token+"&error=Confirmation+required")
		return
	}

	offsets := map[int]int64{partition: offset}
	if err := a.broker.Replay(topicName, groupName, offsets); err != nil {
		c.Redirect(http.StatusFound, "/admin/topic/"+topicName+"?token="+token+"&error="+err.Error())
		return
	}

	c.Redirect(http.StatusFound, "/admin/topic/"+topicName+"?token="+token+"&success=Offset+modified")
}
