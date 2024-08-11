When an API experiences heavy load, we often need to scale it to handle more requests.

A common approach is to allocate additional resources, such as by having AWS or Azure spin up more instances. However, this can be costly, especially when dealing with an API in a monolithic application.

I propose a cost-effective solution that leverages multithreading and a queuing service.

Please refer to the architecture diagram (https://github.com/JourneyOfLearning/A-Journey-of-Learning/wiki) and code example for more details.
