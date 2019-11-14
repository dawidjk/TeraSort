# Tera Sort
The goal of *Tera Sort* was to learn about distributed systems. In order to achieve this, I set out in a mission to make my own little cluster. As soon as the Raspberry Pi 4 had launched, I purchased 4 - eventually acquiring 4 more (my cluster is now at 8 woot!). Together with 5 terabytes worth of disk storage, we can now benchmark the system... But how do we do such a thing you may mask? 
...
Easy!
...
By sorting a terabyte of random 64 bit binary encoded integers. And so, I introduce: *Tera Sort*

# Goals
The ultimate goal of this project is to learn! With that being said, here are the things I set out to accomplish with this project:
- Learn to use Kubernetes
- Experiment with distributed systems
- Improve optimization skills
- Learn Golang
- Build my very own cluster

# The Setup

## Specs

**8x** Raspberry Pi 4 (4gb) - 32gb RAM @ 1.5GHz/node
**2x** Gigabit  network switches
**3x** 1 TB HDD
**1x** 2 TB HDD

## In Progress:
- Merging files after in memory sort

## TODO:

- Network Communication
- Kubernetes Orchestration
- GitHub scoreboard

## Done:
- Benchmark timing
- In memory sort optimization (turns out Go does a pretty good job)
- Load integers from binary file
- Create 1 TB of random integers and store as binary
