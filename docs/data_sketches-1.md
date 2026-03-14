# Part 1

# Data Sketches: The Art of Getting Useful Answers from Massive Data

*How to understand billions of events using only a few kilobytes.*

I recently released `ex_data_sketch`. When two highly technical friends who have spent years working with large-scale data systems heard about it, they asked a simple question:

**What are data sketches?**

They assumed it might be some kind of visualisation technique — perhaps a way to draw data or summarise it graphically.

Instead, data sketches are something far more interesting: tiny probabilistic structures that let us understand enormous datasets without ever storing them in full.

This article is a short introduction to that idea.

---

# A Simple Problem

Imagine you are on the infrastructure team at a streaming platform.

At **9:02 PM**, a new show launches.

Within minutes:

* **12 million viewers** begin watching
* thousands of microservices start emitting telemetry
* millions of requests per second begin hitting your APIs

Your dashboards need to answer questions immediately:

* How many **unique viewers** are watching?
* Which **devices** are most common?
* What is the **99th percentile latency**?
* Is this **request ID new or repeated**?
* Which **error codes are trending**?

The problem is obvious.

Your system is processing **billions of events per hour**.

You cannot keep every event in memory.
You cannot recompute statistics every second.
And yet your monitoring system must still produce answers almost instantly.

Could you store everything and compute exact answers?

Possibly.

But at enormous cost.

At some scale, exact computation stops being interactive. Memory grows linearly. Caches overflow. Disks thrash. Query latency becomes unacceptable.

The traditional solution has been **pre-aggregation**: rolling data up into hourly or daily buckets.

That works for simple metrics like totals or averages.

But it breaks for many important questions.

You can add page views from Monday and Tuesday to get the total for the week.

You **cannot** add Monday’s unique visitors to Tuesday’s and get the correct number of unique visitors for the week — because many users appeared on both days.

This is the famous **count-distinct problem**.

And it is exactly where **data sketches** shine.

---

# What Are Data Sketches?

A **data sketch** is a tiny probabilistic summary of a very large dataset.

It trades perfect accuracy for:

* extremely small memory usage
* extremely fast updates
* mergeability across machines

Think of a sketch as a **statistical memory of a data stream**.

Instead of remembering every event, it remembers just enough structure to answer useful questions.

Like a thumbnail image that captures the essence of a photograph without storing every pixel.

Sketches allow systems to estimate things like:

* number of **unique users**
* **most frequent events**
* **percentiles and quantiles**
* **membership queries**
* **set intersections and unions**

…while using **kilobytes instead of gigabytes**.

The trade-off is accuracy.

But a **1% error answer delivered in milliseconds** is often far more valuable than a perfect answer that arrives minutes later.

Most sketches share four properties:

**Small memory footprint**
They fit easily in streaming pipelines and hot code paths.

**Fast updates**
Millions of events per second are typical.

**Mergeability**
Independent sketches can be combined across distributed systems.

**Bounded error**
Results are approximate, but mathematically predictable.

Once you accept that trade-off, an entire new design space opens up.

---

# The Major Families of Data Sketches

Over time, several families of sketches have emerged.

Each answers a different class of questions.

**Cardinality sketches**
Estimate the number of unique elements.

**Frequency sketches**
Identify heavy hitters in large streams.

**Quantile sketches**
Estimate percentiles such as p95 or p99.

**Membership filters**
Answer the question: “Have we seen this before?”

**Set sketches**
Estimate unions, intersections, and overlaps between datasets.

**Reconciliation sketches**
Recover exact differences between large distributed datasets.

These structures are now embedded throughout modern data infrastructure.

But one example captures the power of the idea particularly well.

---

# One Example That Shows Why Sketches Matter

Suppose you want to count the number of **unique users visiting a website**.

The naive solution is to store every user ID in a set.

That works — until the dataset becomes enormous.

Now consider **HyperLogLog**.

HyperLogLog can estimate the number of unique items in a dataset using **only a few kilobytes of memory**.

A typical configuration might use:

* **16,384 registers**
* roughly **12 KB of RAM**
* about **0.8% error**

With that tiny structure, you can estimate the cardinality of a dataset containing:

**10 thousand items
10 million items
10 billion items**

…with essentially the same error rate.

That is extraordinary.

A structure the size of a small image file can summarise datasets containing **billions of elements**.

This is the moment when most engineers first realise:

**something magical is happening.**

---

# Real-World Uses

Data sketches appear everywhere once you know what to look for.

### Cardinality sketches

Used to estimate:

* unique website visitors
* active hosts in infrastructure
* distinct search queries
* unique telemetry sources

### Frequency sketches

Used to detect:

* trending topics
* popular content
* network traffic anomalies
* fraud signals

### Quantile sketches

Used to compute:

* p95 and p99 latency
* SLA compliance
* tail-latency analysis

### Membership filters

Used in:

* storage engines
* caching systems
* blockchain validators
* distributed databases

They prevent expensive disk lookups for keys that do not exist.

### Set sketches

Used for:

* audience overlap analysis
* marketing attribution
* A/B test segmentation
* behavioural analytics

### Reconciliation sketches

Used in:

* distributed database synchronisation
* blockchain block propagation
* network telemetry
* large-scale data reconciliation

These systems allow huge datasets to stay synchronised without ever transmitting them in full.

---

# The Future of Data Sketches

One of the most powerful properties of sketches is **mergeability**.

Two machines can process different halves of a dataset independently and later merge their sketches into a single summary.

This makes sketches a natural fit for:

* streaming pipelines
* distributed databases
* observability systems
* large-scale analytics platforms

Research in the field continues to move quickly.

New algorithms such as **CPC** push memory efficiency even further.
Hardware acceleration is allowing sketches to run on GPUs and specialised networking hardware.
The intersection with **differential privacy** is particularly promising, allowing systems to produce approximate analytics while guaranteeing strong privacy properties.

Further out, there is even theoretical work exploring **quantum data sketches** — compressing information into quantum states rather than classical memory structures.

The field has grown far beyond its early hashing tricks.

And it is still evolving.

---

# Master Comparison Table of Sketch Algorithms

| Family         | Example Algorithms     | Answers What Question?            | Typical Error    | Mergeable    |
| -------------- | ---------------------- | --------------------------------- | ---------------- | ------------ |
| Cardinality    | HyperLogLog, CPC       | How many unique elements exist?   | ~1%              | Yes          |
| Frequency      | Count-Min, SpaceSaving | Which items appear most often?    | Additive         | Yes          |
| Quantile       | KLL, DDSketch          | What are the percentiles?         | Rank or relative | Yes          |
| Membership     | Bloom, Cuckoo, XOR     | Have we seen this element?        | False positives  | No (usually) |
| Set            | Theta, KMV             | What is the overlap between sets? | ~1–2%            | Yes          |
| Reconciliation | IBLT                   | What differs between datasets?    | Capacity bound   | Yes          |

---

# Final Thought

Data sketches are one of those rare ideas in computer science that feel almost magical.

With only a few kilobytes of memory, they allow us to understand billions of events in real time.

In a world where data continues to grow exponentially, sketches embody a simple but powerful principle:

**You do not need to store everything to understand it.**

Sometimes a clever summary is enough.


