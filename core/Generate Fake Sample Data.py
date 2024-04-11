# Databricks notebook source
# notebook variables

# Blob storage access
# sasToken = "sp=racwl&st=2024-03-27T20:16:36Z&se=2024-03-30T04:16:36Z&spr=https&sv=2022-11-02&sr=c&sig=j5pX5T6%2Fid6hch1OfCJ6xCYpQKMEbdsiYsx3bg5Sw%2FY%3D"
sasToken = 'sp=racwdli&st=2024-04-01T13:13:55Z&se=2024-04-12T21:13:55Z&spr=https&sv=2022-11-02&sr=c&sig=6Lpp4mFsJXUwLFJICnNh%2BPp6i7jokXVNmcGr%2B%2FqI0Cw%3D'

storageAccountName = "gvcfparquet"
storageAccountAccessKey = "none"
blobContainerName = "parquet-files"  

# 3k samples
raw_input_file_path = "data_generation_files"

source_files = f"/mnt/data/{raw_input_file_path}/"


# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount the blob storage so we can get access to the input data needed to generate the fake variants

# COMMAND ----------

# umount the blob storage
if any(mount.mountPoint == "/mnt/data/" for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount("/mnt/data")

# mount the blob storage as /mnt/data
mountPoint = "/mnt/data/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e) 
else: 
  print("Error: mount point exists")

# list the files we just mounted
df = spark.createDataFrame(dbutils.fs.ls(source_files))
display(df)

# COMMAND ----------


weights_file_local_orig = "/dbfs/mnt/data/data_generation_files/gvs_full_vet_weights_1kb_padded_orig.bed"
weights_file_local = "/dbfs/mnt/data/data_generation_files/3k_weights_100b.csv"
spike_file_local = "/dbfs/mnt/data/data_generation_files/3k_outliers_slim_spikey_locations.csv"
bitmap_file_local = "/dbfs/mnt/data/data_generation_files/3k_bitmaps.csv"

reference_local = "/dbfs/mnt/data/data_generation_files/Homo_sapiens_assembly38.fasta"
stats_local = "/dbfs/mnt/data/data_generation_files/3k_bucket_stats_v2.csv"

destinationFolder = "generated_v4"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Required libraries

# COMMAND ----------

!pip install sortedcontainers biopython bitmap

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports
# MAGIC * Csv and gzip are used to read in the various statistics file
# MAGIC * Random and numpy are used for choosing where to place a variant and also generating data from a mean + standard deviation
# MAGIC * Bio.Seq is used to read in the reference data from a fasta
# MAGIC * sortedContainers is needed to set up the data structure that lets us quickly finding where to put a variant

# COMMAND ----------

import csv
import gzip
import random
import numpy as np

from Bio import SeqIO
from itertools import islice
from sortedcontainers import SortedDict
from bitmap import BitMap


# COMMAND ----------

statsBlockSize = 10000
# weightBlockSize = 1000
weightBlockSize = 100

# COMMAND ----------

# MAGIC %md
# MAGIC #Double-checking the bit manipulating code manually against data known to be in the db below. 
# MAGIC (Leave commented out)

# COMMAND ----------


# # high_bits = 937204812610031
# # low_bits = 752945729

# high_bits = 204108115665822
# low_bits = 1072811924033080

# format_string = '{0:050b}'


# # print(f"high bits = {bin(high_bits)}{padding}")
# # print(f"low bits = {bin(low_bits)}")
# highBitsPaddedString = format_string.format(high_bits)
# lowBitsPaddedString = format_string.format(low_bits)
# allBitsString = highBitsPaddedString+lowBitsPaddedString
# bm = BitMap.fromstring(allBitsString)

# print(f"high bits = {highBitsPaddedString}")
# print(f"low bits = {lowBitsPaddedString}")

# # print(f"all bits: {highBitsPaddedString + lowBitsPaddedString}")
# # all_bits = f"{highBitsPaddedString}{lowBitsPaddedString}" 
# # print(f"all bits: {all_bits}")


# # bm = BitMap(100)
# bm = BitMap.fromstring(allBitsString)
# bm.tostring()
# len(bm.nonzero())

# # if 5 in bm.nonzero():
# #     print("found it!")

# COMMAND ----------


def convertToLocation(contig, position):
        # Split the contig to get the number out of it
        pieces = contig.lower().split("r")
        contigNumber = 0
        match pieces[1]:
            case "x":
                contigNumber = 23
            case "y":
                contigNumber = 24
            case "m":
                contigNumber = 25
            case _:
                contigNumber = int(pieces[1])
        
        return contigNumber * 1000000000000 + position;
    
def decomposeLocation(location):
        # Split the contig to get the number out of it
        contigNumber = int(location / 1000000000000)
        position = location % 1000000000000
        contig = "chr"
        match contigNumber:
            case 23:
                contig = contig + "X"
            case 24:
                contig = contig + "Y"
            case 25:
                contig = contig + "M"
            case _:
                contig = contig + str(contigNumber)
        
        return {'contig':contig, 'pos':position}

def closest(sorted_dict, key):
    "Return closest key in `sorted_dict` to given `key`."
    assert len(sorted_dict) > 0
    keys = list(islice(sorted_dict.irange(minimum=key), 1))
    return keys[0]

def getBitmapForBits(low_bits, higg_bits):
    format_string = '{0:050b}'
    highBitsPaddedString = format_string.format(high_bits)
    lowBitsPaddedString = format_string.format(low_bits)
    allBitsString = highBitsPaddedString+lowBitsPaddedString
    return BitMap.fromstring(allBitsString)

# COMMAND ----------

hg38_dict = SeqIO.to_dict(SeqIO.parse(reference_local, "fasta"))
print(hg38_dict["chr1"])

# COMMAND ----------

print(hg38_dict)

# COMMAND ----------

statsCount = 0
statsSkip = 1000
statsMap = {}
with gzip.open(stats_local, mode="rt") as statsFile:
    statsReader = csv.reader(statsFile, delimiter=',')
    next(statsReader, None)  # skip the headers
    for row in statsReader:
        locationBin = int(row[0])
        entries = int(row[1])
        
        # some of the stats fail in some bins where there's literally just one value. Throw an arbitrary number in there
        try:
            avg_qual = float(row[2])
        except ValueError:
            avg_qual = 5
        try:
            stddev_qual = float(row[3])
        except ValueError:
            stddev_qual = 5
        try:
            avg_refad = float(row[4])
        except ValueError:
            avg_refad = 5
        try:
            stddev_refad = float(row[5])
        except ValueError:
            stddev_refad = 5
        try:
            avg_ad = float(row[6])
        except ValueError:
            avg_ad = 5
        try:
            stddev_ad = float(row[7])
        except ValueError:
            stddev_ad = 5
        try:
            avg_gq = float(row[8])
        except ValueError:
            avg_gq = 5
        try:
            stddev_gq = float(row[9])
        except ValueError:
            stddev_gq = 5
        
        
        if statsCount % statsSkip == 0:
#             print(f"statsRow: {row}")
            print(f"locationBin = {locationBin} \tentries = {entries} \tavg_qual = {avg_qual} \tstddev_qual = {stddev_qual} \tavg_refad = {avg_refad} \tstddev_refad = {stddev_refad} \tavg_ad = {avg_ad} \tstddev_ad = {stddev_ad} \tavg_gq = {avg_gq} \tstddev_gq = {stddev_gq}") 
        
        statsMap[locationBin] = {'avg_qual':avg_qual, 'stddev_qual':stddev_qual, 'avg_refad':avg_refad, 'stddev_refad':stddev_refad, 'avg_ad':avg_ad, 'stddev_ad':stddev_ad, 'avg_gq':avg_gq, 'stddev_gq':stddev_gq}
        # statsMap[locationBin] = {'locationBin':locationBin, 'entries':entries, 'avg_qual':avg_qual, 'stddev_qual':stddev_qual, 'avg_refad':avg_refad, 'stddev_refad':stddev_refad, 'avg_ad':avg_ad, 'stddev_ad':stddev_ad, 'avg_gq':avg_gq, 'stddev_gq':stddev_gq}
        statsCount += 1


# COMMAND ----------


# using old weights file format...

# total_weight = 0
# with open (weights_file_local, newline='') as bedfile:
#     bedreader = csv.reader(bedfile, delimiter='\t')
#     for row in bedreader:
#         contig = row[0]
#         start = int(row[1])
#         ending = int(row[2])
#         weight = int(row[4])
# #         print(f"row: {row}")
# #         print(f"Block {contig}: {start} - {ending}")
#         if weight != 0:
# #             print(f"would add block {contig}: {start} - {ending}")
#             total_weight += weight
    
# print(f"Total entries: {total_weight}")

# COMMAND ----------

# read in the map where we store any variants that are very "spiky" in a given interval
spikeyMap = {}
total_spikes = 0
with gzip.open(spike_file_local, mode="rt") as bedfile:
    bedreader = csv.reader(bedfile, delimiter=',')
    next(bedreader, None)  # skip the headers
    for row in bedreader:
        bin = int(row[0])
        if not row[1]:
            # print(f"Skipping row...")
            continue
        location = int(row[1])
        allele = row[2]
        percentage = float(row[3])
        spikeyMap[bin] = {'location':location,'allele':allele,'percentage':percentage}
        # spikeyMap[bin] = {'bin':bin,'location':location,'allele':allele,'percentage':percentage}
        total_spikes = total_spikes + 1

print(f"Stored data for {total_spikes} spikes")

# COMMAND ----------

# read in the map where we store the bitmaps for the actual known variants 
knownVariants = {}
with gzip.open(bitmap_file_local, mode="rt") as bedfile:
    bedreader = csv.reader(bedfile, delimiter=',')
    next(bedreader, None)  # skip the headers
    for row in bedreader:
        bin = int(row[0])
        
        low_bits = 0
        high_bits = 0
        
        if row[2]:
            low_bits = int(row[2])
        
        if row[3]:
            high_bits = int(row[3])

        # bm = getBitmapForBits(low_bits, high_bits)
        knownVariants[bin] = [low_bits, high_bits]
        # knownVariants[bin] = {'low':low_bits, 'high':high_bits}

print(f"Done storing bitmaps for all intervals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Because I have run this entire file a million times, I bypassed the actual calculation to just set the weight variable.

# COMMAND ----------

# the new weights file comes as a gzipped csv

total_weight = 17147924134
# UNCOMMENT STARTING HERE
# total_weight = 0
# with gzip.open(weights_file_local, mode="rt") as bedfile:
#     bedreader = csv.reader(bedfile, delimiter=',')
#     next(bedreader, None)  # skip the headers
#     for row in bedreader:
#         # print(f"row: {row}")
#         # contig = row[0]
#         # start = int(row[1])
#         # ending = int(row[2])
#         # weight = int(row[4])
#         location = int(row[0])
#         weight = int(row[1])
#         decomposed = decomposeLocation(location)
#         contig = decomposed['contig']
#         start = decomposed['pos']
#         ending = start + weightBlockSize
#         # weight = int(row[4])

#         if weight != 0:
# #             print(f"would add block {contig}: {start} - {ending}")
#             total_weight += weight

# UNCOMMENT ENDING HERE

print(f"Total entries: {total_weight}")




# COMMAND ----------

# the old way...
# printSkip = 10000
# currentCount = 0
# cumulativePercentage = 0.0

# segmentDictionary = SortedDict()

# with open (weights_file_local, newline='') as bedfile:
#     bedreader = csv.reader(bedfile, delimiter='\t')
#     for row in bedreader:
#         contig = row[0]
#         start = int(row[1])
#         ending = int(row[2])
#         weight = int(row[4])
# #         print(f"row: {row}")
# #         print(f"Block {contig}: {start} - {ending}")
#         if weight != 0:
#             percentage = weight / total_weight;
#             cumulativePercentage = cumulativePercentage + percentage
#             segmentDictionary[cumulativePercentage]={'contig':contig, 'start':start, 'end':ending, 'weight':weight, 'p':percentage, 'cp':cumulativePercentage, "location":convertToLocation(contig, start)}
#             if currentCount % printSkip == 0:
#                 print(f"({currentCount}) {contig}: {start} - {ending}: {percentage} (cumulative: {cumulativePercentage})")
        
#         currentCount = currentCount + 1
        

# COMMAND ----------

printSkip = 10000
currentCount = 0
cumulativePercentage = 0.0

segmentDictionary = SortedDict()

with gzip.open(weights_file_local, mode="rt") as bedfile:
    bedreader = csv.reader(bedfile, delimiter=',')
    next(bedreader, None)  # skip the headers
    for row in bedreader:
        location = int(row[0])
        weight = int(row[1])
        decomposed = decomposeLocation(location)
        contig = decomposed['contig']
        start = decomposed['pos']
        ending = start + weightBlockSize
        if weight != 0:
            percentage = weight / total_weight;
            cumulativePercentage = cumulativePercentage + percentage
            # segmentDictionary[cumulativePercentage]={'contig':contig, 'start':start, 'end':ending, 'weight':weight, 'p':percentage, 'cp':cumulativePercentage, "location":location}
            # Let's trim this down to save a lot of space
            segmentDictionary[cumulativePercentage]={'end':ending, "location":location}
            if currentCount % printSkip == 0:
                print(f"({currentCount}) {contig}: {start} - {ending}: {percentage} (cumulative: {cumulativePercentage})")
        
        currentCount = currentCount + 1
        

# COMMAND ----------

# This tests getting a the bucket above but closest to 0.5. The "cp" field, or cumulative probability, should reflect this if our data struture was created correctly
nearestKey = closest(segmentDictionary, 0.5)
elementForGeneration = segmentDictionary[nearestKey]
print(elementForGeneration)

# COMMAND ----------

# This tests getting a weighted random bucket into which we should be generating our fake SNP.
nearestKey = closest(segmentDictionary, random.uniform(0,1))
elementForGeneration = segmentDictionary[nearestKey]
print(elementForGeneration)

# COMMAND ----------

# destinationFolder = "generated_v3"

# COMMAND ----------

! mkdir /dbfs/{destinationFolder}
! ls -al /dbfs/

# COMMAND ----------

knownVariants.keys()

# COMMAND ----------


# generate a bunch of ordered, fake SNPs for this user
transitions = {"a":"g", "g":"a", "c":"t", "t":"c"}
transversion1 = {"a":"c", "g":"c", "c":"a", "t":"a"}
transversion2 = {"a":"t", "g":"t", "c":"g", "t":"g"}

allowedBases = "gatc"
# sample_id = 50
totalSNPs = 10
realSampleSNPs = 5360000
# The original plan for generating 10 samples.
# for sample_id in range(50, 60):
# generate 100 this time
for sample_id in range(1, 100):
    printSkip = 100000
    currentCount = 0
    with open(f"/dbfs/{destinationFolder}/sample_{sample_id}.csv", 'w') as csvOutputFile:
        fakeDataWriter = csv.writer(csvOutputFile, delimiter='\t')
        fakeDataWriter.writerow(["location","sample_id","ref","allele","allele_pos","call_GT","call_GQ","as_raw_mq","raw_mq","as_raw_mqranksum","qual","as_raw_readposranksum","as_sb_table","call_AD","ref_ad","ad"])  
        # for i in range(1, totalSNPs):
        for i in range(1, realSampleSNPs):
            nearestKey = closest(segmentDictionary, random.uniform(0,1))
            elementForGeneration = segmentDictionary[nearestKey]

            bucket = elementForGeneration['location']

            # decompose it so we get the corrected form of the contig
            decomposed = decomposeLocation(bucket)
            contig = decomposed['contig']
            bucketStartPosition = decomposed['pos']
            
            contigSequence = hg38_dict[contig]

            # Is this one of the spikey buckets where one mutation dominates all other?  And if so, should we use it?
            if bucket in spikeyMap and random.uniform(0,1) < spikeyMap[bucket]['percentage']:
                # This is what spikeyMap content looks like, for reference
                # {'bin':bin,'location':location,'allele':allele,'percentage':percentage}
                prominentSNP = spikeyMap[bucket]
                finalLocation = prominentSNP['location']
                finalPosition = decomposeLocation(finalLocation)['pos']
                allele = prominentSNP['allele']
                refBase = contigSequence[finalPosition].lower()
            else:
                # generate it the old way, which is kinda randomly
                # use a bitmap for all active sites
                goodLocationsArray = knownVariants[bucket]
                goodLocations = getBitmapForBits(goodLocationsArray[0], goodLocationsArray[1])

                # for the first 100, just choose randomly within the bucket.  For the rest, choose from seen ones at that location
                if currentCount < 100:
                    subPosition = random.randint(0,weightBlockSize - 1)
                else:
                    subPosition = random.choice(goodLocations.nonzero())
                
                finalPosition = bucketStartPosition + subPosition
                finalLocation = bucket + subPosition
                snpRoll = random.randint(1, 6)
                typeOfSnp = ""
                allele = ""
                
                refBase = contigSequence[finalPosition].lower()
                # we get weird shit out of the official reference in some location...
                if allowedBases.find(refBase) == -1:
                    continue
                
                # if refBase == "n" or refBase == 'y' or refBase = 'r':
                #     continue
                
                if snpRoll <= 4:
                    # 0 is a transition snip
                    typeOfSnp = "Ti"
                    allele = transitions[refBase]
                elif snpRoll == 5:
                    typeOfSnp = "Tv1"
                    allele = transversion1[refBase]
                else:
                    typeOfSnp = "Tv2"
                    allele = transversion2[refBase]
            

            # Get the statistics block for this.  Gotta round to the lower 10k to find it
            statsKey = finalLocation - finalLocation % statsBlockSize
            if statsKey not in statsMap:
                continue
            statBlock = statsMap[statsKey]
            # print(f"({finalLocation}){elementForGeneration['contig']}:{finalPosition} - {typeOfSnp}")

            generated_qual = round(np.random.normal(loc=statBlock['avg_qual'], scale=statBlock['stddev_qual']))
            generated_refad = round(np.random.normal(loc=statBlock['avg_refad'], scale=statBlock['stddev_refad']))
            generated_ad = round(np.random.normal(loc=statBlock['avg_ad'], scale=statBlock['stddev_ad']))
            generated_gq = round(np.random.normal(loc=statBlock['avg_gq'], scale=statBlock['stddev_gq']))
            
            # bound the values within a rational range
            if generated_qual <= 0:
                generated_qual = 0
            if generated_refad <= 0:
                generated_refad = 1
            if generated_ad <= 0:
                generated_ad = 1
                
            if generated_gq <= 0:
                generated_gq = 1
            elif generated_gq > 100:
                generated_gq = 100

            # give them all a fake genotype of 0/1 for now
            call_AD = f"{generated_refad},{generated_ad}"
            fake_gt = "0/1"
            alle_pos = 1
            # "location","sample_id","ref","allele","allele_pos","call_GT","call_GQ","as_raw_mq","raw_mq","as_raw_mqranksum","qual","as_raw_readposranksum","as_sb_table","call_AD","ref_ad","ad"
            fakeDataWriter.writerow([finalLocation, sample_id, refBase.upper(), allele.upper(), alle_pos, fake_gt, generated_gq, None, None, None, generated_qual, None, None, call_AD, generated_refad, generated_ad])
            
            # print(f"location={finalLocation}\tsample_id={sample_id}\tref={refBase}\tallele={allele}\tallele_pos={alle_pos}\tfake_gt = {fake_gt}\tgenerated_qual = {generated_qual}\tgenerated_gq = {generated_gq}\tcall_AD = {call_AD}\tgenerated_refad = {generated_refad}\tgenerated_ad = {generated_ad}")
            if currentCount % 100000 == 0:
                print(f"Sample id {sample_id}: {currentCount} out of {realSampleSNPs}...")
            currentCount = currentCount + 1


    


# COMMAND ----------

!ls -la /dbfs/{destinationFolder}

# COMMAND ----------

!ls -la /dbfs/generated_v0

# COMMAND ----------

! head /dbfs/{destinationFolder}/sample_58.csv

# COMMAND ----------

from pyspark.sql.functions import col

df = (
    spark.read.option("header", "true")
    .option("sep", "\t")
    .option("multiLine", "false")
    .option("quote", '"')
    .option("escape", '"')
    .option("ignoreTrailingWhiteSpace", True)
    # .csv("dbfs:/generated_v1")
    .csv(f"dbfs:/{destinationFolder}")
)

for column in ["location","sample_id","allele_pos","call_GQ","qual","ref_ad","ad"]:
    df = df.withColumn(column, col(column).cast("long"))
display(df)

# COMMAND ----------


# read the schema from the alt_allele table which should be empty (probably don't need this)
alt_allele = spark.read.table("alt_allele")

# verify it is empty
if alt_allele.count() > 0: 
    print("Error: alt_allele is not empty, truncate it")
    exit(1)

# union the new fake sample data with alt_allele to make sure we have the correct schema
combined = alt_allele.union(df)

display(combined.where("sample_id = 58"))

# COMMAND ----------

alt_allele_w_fake = "alt_allele_w_fake_v4" 

# drop the fake allele table
spark.sql(f"DROP TABLE IF EXISTS {alt_allele_w_fake}")

# create the fake alele table from the new sample data 
combined.writeTo(alt_allele_w_fake).using("delta").clusterBy("location").create()


# COMMAND ----------

# now optimize it
spark.sql(f"OPTIMIZE {alt_allele_w_fake}")