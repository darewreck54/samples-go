package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/temporalio/samples-go/mutex"
)

func main() {
	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	// This workflow ID can be user business logic identifier as well.
	resourceID := "mutex_resource"
	workflow1Options := client.StartWorkflowOptions{
		ID:        "A",
		TaskQueue: "mutex",
	}

	workflow2Options := client.StartWorkflowOptions{
		ID:        "B",
		TaskQueue: "mutex",
	}

	workflow3Options := client.StartWorkflowOptions{
		ID:        "C",
		TaskQueue: "mutex",
	}

	workflow4Options := client.StartWorkflowOptions{
		ID:        "D",
		TaskQueue: "mutex",
	}
	we, err := c.ExecuteWorkflow(context.Background(), workflow1Options, mutex.SampleWorkflowWithMutex, resourceID)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	} else {
		log.Println(fmt.Sprintf("Started workflow [%s]", we.GetID()))
	}

	time.Sleep(2 * time.Second)
	we, err = c.ExecuteWorkflow(context.Background(), workflow2Options, mutex.SampleWorkflowWithMutex, resourceID)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	} else {
		log.Println(fmt.Sprintf("Started workflow [%s]", we.GetID()))
	}
	time.Sleep(2 * time.Second)
	we, err = c.ExecuteWorkflow(context.Background(), workflow3Options, mutex.SampleWorkflowWithMutex, resourceID)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	} else {
		log.Println(fmt.Sprintf("Started workflow [%s]", we.GetID()))
	}

	time.Sleep(2 * time.Second)
	we, err = c.ExecuteWorkflow(context.Background(), workflow4Options, mutex.SampleWorkflowWithMutex, resourceID)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	} else {
		log.Println(fmt.Sprintf("Started workflow [%s]", we.GetID()))
	}
}
