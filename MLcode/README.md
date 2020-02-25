Wikipedia Word Vectors based classifier which caches word vectors of categories in memcached..and generate classification  of urls by deriving refined word vectors of web pages.
Fast text libaray is used to generate wikipedia word vectors in English language.
This data can be updated with recent wikipedia dump.
Classification is done by comparing cosine distance with category vectors present in category List file. These vectors are cached in memcached. Redis does not seem to work too well with word vectors. 

GensimModuleCodev9.py - For calling SemanticClassifierv2 API based on TagMe.
