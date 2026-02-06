COLLOCATION EXTRACTION

**Authors:** ESHED HAYOUN   

---

## TABLE OF CONTENTS
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Running the Code](#running-the-code)
5. [Downloading Results](#downloading-results)
6. [Creating Required Reports](#creating-required-reports)
7. [Algorithm Details](#algorithm-details)
8. [Design Decisions](#design-decisions)

---

## OVERVIEW

This MapReduce program extracts significant collocations (word pairs) from Google N-grams using the Log-Likelihood Ratio (LLR) statistical measure. It processes both English and Hebrew corpora, outputting the top-100 most significant word pairs per decade.

---

## PREREQUISITES

### 1. AWS Account & Credentials

You need an AWS account with permissions to:
- Create and manage EMR clusters
- Read/write to S3
- Access the Google N-grams public dataset

### 2. Configure AWS Credentials

**On your local machine, run:**

- export AWS_ACCESS_KEY_ID: [Your access key]
- export AWS_SECRET_ACCESS_KEY: [Your secret key]
- export AWS_SESSION_TOKEN: [Your session token]

### 3. Install Required Software

- **Java 8 or higher:** `java -version`
- **Maven:** `mvn -version`
- **AWS CLI:** `aws --version`

---

## SETUP INSTRUCTIONS

### Step 1: Set Your S3 Bucket Name

**Create a unique S3 bucket:**
```bash
BUCKET="YOUR-BUGKET-NAME"
aws s3 mb s3://$BUCKET --region us-east-1
echo "Your bucket: $BUCKET"
```

**IMPORTANT:** Save this bucket name! You'll use it throughout.


### Step 2: Build the JAR
```bash
cd /path/to/your/project
mvn clean package
```

**Output:** `target/llr-collocation-1.0.0.jar`

### Step 3: Upload Files to S3
```bash
# Upload JAR
aws s3 cp target/llr-collocation-1.0.0.jar s3://$BUCKET/jars/

# Upload stopwords
aws s3 cp eng-stopwords.txt s3://$BUCKET/stopwords/
aws s3 cp heb-stopwords.txt s3://$BUCKET/stopwords/

# Verify uploads
aws s3 ls s3://$BUCKET/jars/
aws s3 ls s3://$BUCKET/stopwords/
```

---

## RUNNING THE CODE

### Run English Corpus
```bash

aws emr create-cluster \
  --name "English-Collocations" \
  --release-label emr-5.36.0 \
  --applications Name=Hadoop \
  --instance-type m4.large \
  --instance-count 5 \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
  --region us-east-1 \
  --log-uri s3://$BUCKET/logs/ \
  --steps Type=CUSTOM_JAR,Name="English",Jar=s3://$BUCKET/jars/llr-collocation-1.0.0.jar,Args=\[s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data,s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data,s3://$BUCKET/output/ENGLISH,s3://$BUCKET/stopwords/eng-stopwords.txt\] \
  --auto-terminate

echo "English job started! Monitor: https://console.aws.amazon.com/emr/"
```

**Expected runtime:** ~1.5 hours  

### Run Hebrew Corpus
```bash

aws emr create-cluster \
  --name "Hebrew-Collocations" \
  --release-label emr-5.36.0 \
  --applications Name=Hadoop \
  --instance-type m4.large \
  --instance-count 5 \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
  --region us-east-1 \
  --log-uri s3://$BUCKET/logs/ \
  --steps Type=CUSTOM_JAR,Name="Hebrew",Jar=s3://$BUCKET/jars/llr-collocation-1.0.0.jar,Args=\[s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data,s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data,s3://$BUCKET/output/HEBREW,s3://$BUCKET/stopwords/heb-stopwords.txt\] \
  --auto-terminate

echo "Hebrew job started!"
```

**Expected runtime:** ~25-30 minutes

---

## DOWNLOADING RESULTS

### Download English Results
```bash

# Download results
rm -f part-r-* ENGLISH-RESULTS.txt
aws s3 cp s3://$BUCKET/output/ENGLISH/final_result/ ./ --recursive --exclude "*" --include "part-r-*"

# Combine into single file
cat part-r-* > ENGLISH-RESULTS.txt
rm part-r-*

# Verify count
wc -l ENGLISH-RESULTS.txt
# Expected: 4700 ENGLISH-RESULTS.txt
```

### Download Hebrew Results
```bash
# Download results
rm -f part-r-* HEBREW-RESULTS.txt
aws s3 cp s3://$BUCKET/output/HEBREW/final_result/ ./ --recursive --exclude "*" --include "part-r-*"

# Combine into single file
cat part-r-* > HEBREW-RESULTS.txt
rm part-r-*

# Verify count
wc -l HEBREW-RESULTS.txt
# Expected: 3300 HEBREW-RESULTS.txt
```

---

## CREATING REQUIRED REPORTS

### REPORT 1: Statistics (With/Without Local Aggregation)

**Step 1: Extract statistics from logs**
```bash
# Find cluster ID
aws emr list-clusters --region us-east-1 | grep "English-Collocations"

# Set cluster ID
CLUSTER_ID="j-XXXXXXXXXXXXX"

# Get step ID
STEP_ID=$(aws emr list-steps --cluster-id $CLUSTER_ID --query 'Steps[0].Id' --output text)

# Download Job 1 log
aws s3 ls s3://$BUCKET/logs/$CLUSTER_ID/containers/ --recursive | grep "application.*0001.*01_000001.*syslog"

# Download and extract
aws s3 cp s3://$BUCKET/logs/$CLUSTER_ID/containers/application_XXX_0001/container_XXX_0001_01_000001/syslog.gz ./job1-log.gz
gunzip job1-log.gz

# Extract statistics
grep -A 80 "Counters:" job1-log | grep -E "Map output records|Combine input records|Combine output records|Reduce input records"
```

**Step 2: Create document with:**
```
STATISTICS REPORT

Job 1: Calculate N
Map output records:      44,400,490
Combine input records:   44,400,490
Combine output records:  495
Reduce input records:    495
Reduction: 99.99%

Job 2: Join with c1
Map output records:      159,586,657
Reduce output records:   115,557,585
Intermediate data: 4.62 GB

Job 3: Calculate LLR
Map output records:      159,586,657
Reduce output records:   4,700 (English), 3,300 (Hebrew)
```

---

### REPORT 2: Good & Bad Collocations

**Finding GOOD collocations:**
```bash
# Top English collocations
sort -t$'\t' -k2 -rn ENGLISH-RESULTS.txt | head -50

# Search for meaningful pairs
grep -i "New-York\|United-States\|World-War" ENGLISH-RESULTS.txt

# Top Hebrew collocations
sort -t$'\t' -k2 -rn HEBREW-RESULTS.txt | head -50
```

**Criteria for GOOD:**
- ✅ Proper nouns (cities, countries, names)
- ✅ Meaningful phrases with semantic content
- ✅ Official titles or terminology

**Finding BAD collocations:**
```bash
# Look for OCR errors and citations
head -200 ENGLISH-RESULTS.txt | less
grep -E "et-al|weale|thofe|thy-felfe" ENGLISH-RESULTS.txt
```

**Criteria for BAD:**
- ❌ Citation markers (et-al)
- ❌ OCR errors (broken words)
- ❌ Latin fragments
- ❌ Non-semantic pairs

**Create document with 10 good + 10 bad examples for each language with explanations.**

---

### REPORT 3: Manual Analysis

**Create document explaining why bad collocations appear:**
```
MANUAL ANALYSIS

1. CITATION MARKERS
   - "et-al" appears 7,983,395 times
   - Academic formatting, not semantic meaning

2. OCR ERRORS
   - Long 's' (ſ) misread as 'f'
   - "thy-felfe" should be "thyself"
   - Gothic fonts confuse OCR

3. MULTILINGUAL TEXT
   - Latin phrases in English books
   - "cum-notis", "fieri-potuit"
   
4. HISTORICAL TYPOGRAPHY
   - Pre-1800 texts use different conventions
   - Systematic OCR errors in old books

5. DOMAIN JARGON
   - Religious/legal terminology
   - High frequency within domain only
```

---

## ALGORITHM DETAILS

### Job 1: Calculate N
- **Input:** 1-gram data (word, year, count)
- **Output:** Decade → N (total words)
- **Combiner:** YES (99.99% reduction)

### Job 2: Join with c1
- **Input:** 1-gram + 2-gram data
- **Output:** decade:w2 → partial(w1, c1, c12)
- **Combiner:** NO (cross-mapper join)

### Job 3: Calculate LLR
- **Input:** Job2 output + 1-gram (for c2)
- **Output:** Top-100 per decade, sorted by LLR
- **Combiner:** NO (priority queue)
- **Partitioner:** DecadePartitioner

---

## DESIGN DECISIONS

### 1. DecadePartitioner
Routes all pairs from same decade to same reducer, ensuring correct top-100 selection.

### 2. Combiner in Job 1 Only
99.99% reduction in Job 1. Cannot use in Job 2 (join) or Job 3 (priority queue).

### 3. Priority Queue
Maintains only top-100 in memory. O(n log k) vs O(n log n) for full sort.

### 4. Unicode Pattern
Changed from `^[A-Za-z]+$` to `.*\\p{L}.*` to support Hebrew.

### 5. Stopwords Filtering
HashSet lookup O(1). Removes high-frequency function words.

---

## PROJECT STRUCTURE
```
DSP-Assignment-2/
├── reports/
│   ├── ANALYSIS.txt
│   └── STATISTICS.txt
├── src/main/java/com/example/llr/
│   ├── CollocationDriverWithCombiner.java
│   ├── WordCountStepWithCombiner.java
│   ├── Job2BigramMapper.java
│   ├── Job2UnigramMapper.java
│   ├── Job2Reducer.java
│   ├── Job3PartialMapper.java
│   ├── Job3UnigramMapper.java
│   ├── Job3Reducer.java
│   ├── DecadePartitioner.java
│   ├── NLPUtils.java
│   └── LLRCalculator.java
├── pom.xml
├── eng-stopwords.txt
├── heb-stopwords.txt
└── README.md
```

---

## EXPECTED RESULTS

**English:** 4,700 collocations (47 decades × 100)  
**Hebrew:** 3,300 collocations (33 decades × 100)  
**Runtime:** English ~1.5h, Hebrew ~25-30min

---

## TROUBLESHOOTING

**EMR fails:** Check IAM roles exist  
**Wrong results:** Verify stopwords uploaded  
**Hebrew shows Roman numerals:** Use fixed JAR with Unicode pattern

---

## COST ESTIMATE

English + Hebrew: ~$3-5 total

---

## Potential Optimizations and Scalability Improvements

### 1. Pre-compute Word Counts (4-Job Architecture)
**Issue:** The current implementation reads 1-gram data twice - once in Job 2 for c1 and once in Job 3 for c2.

**Solution:** Add Job 1a to calculate all word counts once:
- **Job 1a:** Process 1-grams → output (decade:word, count)
- **Job 1b:** Aggregate word counts → calculate N values
- **Jobs 2 & 3:** Read pre-computed word counts instead of raw 1-grams

**Benefits:**
- Reduces total 1-gram reads from 3 to 1
- Enables reusability for multiple analyses
- Adds combiner to Job 1a for efficiency (reduces network I/O by ~87%)

**Trade-offs:**
- Adds ~500MB-1GB intermediate storage
- Requires 4 jobs instead of 3
- Beneficial when running pipeline multiple times

### 2. Add Combiner to Bigram Processing
**Issue:** Job 2's bigram mapper produces high intermediate output without local aggregation.

**Solution:** Implement a combiner for Job 2's bigram processing to aggregate locally before shuffle phase.

**Benefits:**
- Reduces network transfer during shuffle
- Decreases reducer input size
- Improves overall Job 2 performance

### 3. Persistent N Values Storage
**Issue:** N values are recalculated on every pipeline run, even though they're small (~50 records, 430 bytes).

**Solution:**
- Calculate N once and save to dedicated S3 location
- Subsequent runs read N values directly from S3
- Optionally skip Job 1 entirely if N values exist

**Benefits:**
- Faster pipeline iterations during development
- Reduces computation for repeated analyses

### 4. Scalable Top-K Selection (Addressing DecadePartitioner Limitation)
**Issue:** `DecadePartitioner` assumes reducers ≥ decades. For 1000+ decades, this approach doesn't scale.

**Solution:** Two-stage reduction for Job 3:
- **Stage 1:** Calculate all LLR scores (no top-100 selection)
- **Stage 2:** Use DecadePartitioner + separate reducer per decade for top-100 selection
