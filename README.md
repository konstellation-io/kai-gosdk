# KAI Golang Runner

This is an implementation in Go for the KAI runner.

## How it works

The Go runner is one of the two types of runners that can be used in a KAI workflow and allows
executing go code.

Once the go runner is deployed, it connects to NATS and subscribes permanently to an input
subject.
Each node knows to which subject it has to subscribe and also to which subject it has to send messages,
since the [K8s manager](https://github.com/konstellation-io/kai/tree/main/engine/k8s-manager) tells it with environment variables.
It's important to note that the nodes use a queue subscription,
which allows load balancing of messages when there are multiple replicas of the runner.

When a new message is published in the input subject of a node, the runner passes it down to a
handler function, along with a context object formed by variables and useful methods for processing data.
This handler is the solution implemented by the client and given in the krt file generated.
Once executed, the result will be taken by the runner and transformed into a NATS message that
will then be published to the next node's subject (indicated by an environment variable).
After that, the node ACKs the message manually.                                           |

## Run Tests

Execute the tests running in the root folder:

``` sh
make tests
```

## Run Linter

Execute the linter running in the root folder:

``` sh
make tidy
```

### Update mocks

Update the mocks running in the root folder:

``` sh
make generate_mocks
```

### Make protos

Generate the protos running in the root folder:

``` sh
make protos
```