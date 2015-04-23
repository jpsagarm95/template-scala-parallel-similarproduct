# Similar Product Template

##Overview

This is a template aimed at integrating PredictionIO with the Mahout-Spark's ItemSimilarity Library.
However, owing to a Javaserializer bug in Spark 1.2+, this module is still incomplete.(Please find a description of the error here: http://mail-archives.apache.org/mod_mbox/mahout-user/201409.mbox/%3CCAJBgT0=zC1XP-WozTkZF=EPVCPGj+1qXR3=MykCHr42yuT4m-A@mail.gmail.com%3E)

##Changes Made to the Template

###DataSource.scala
No changes were made here.

###Preparator.scala
We extracted the unique users into a map (users), and the unique items into a map (items). This allows us to associate the string value (got from the data) to an integer value.
We also built a HashBiMap (defined in the Google guava libraries) for both of these maps. These are required to build the indexedDataset.
We also needed a ArrayBuffer[DrmTuple[Int]] containing the viewEvents. As the rows/columns of this matrix are indexed by the values in the users and items maps, we sorted these two. We then iterated over all users and items to create this matrix. We have used DenseVector as each row in the matrix. 
We then built an indexedDatasetSpark using an RDD created from the matrix, and the user and item biMaps.  
We pass the indexedDataset along with the items biMap to COSAlgorithm.scala

###COSAlgorithm.scala
We called the function SimilarityAnalysis.cooccurrencesIDSs, passing the userItemMatrix and other algorithmic parameters. This gives the serialization error.

We were unable to proceed any further.

 
### import sample data

```
$ python data/import_eventserver.py --access_key <your_access_key>
```

