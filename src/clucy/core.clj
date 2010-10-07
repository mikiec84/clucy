(ns clucy.core
  (:use clojure.contrib.java-utils)
  (:import java.io.File
	   java.io.StringReader
           org.apache.lucene.document.Document
           (org.apache.lucene.document Field Field$Store Field$Index)
           (org.apache.lucene.index IndexWriter IndexWriter$MaxFieldLength
				    IndexReader)
	   org.apache.tika.language.LanguageIdentifier
           ;;org.apache.lucene.analysis.standard.StandardAnalyzer
	   org.apache.lucene.analysis.snowball.SnowballAnalyzer
	   org.apache.lucene.analysis.tokenattributes.TermAttribute
	   org.apache.lucene.analysis.tokenattributes.TermAttributeImpl
	   org.apache.lucene.analysis.tokenattributes.TypeAttribute
	   org.apache.lucene.analysis.tokenattributes.TypeAttributeImpl
           org.apache.lucene.queryParser.QueryParser
           org.apache.lucene.search.IndexSearcher
           (org.apache.lucene.store RAMDirectory NIOFSDirectory)
           org.apache.lucene.util.Version
           org.apache.lucene.search.BooleanQuery
           org.apache.lucene.search.BooleanClause
           org.apache.lucene.search.BooleanClause$Occur
           org.apache.lucene.index.Term
           org.apache.lucene.search.TermQuery))

(def *version*  Version/LUCENE_30)
;;(def *analyzer* (StandardAnalyzer. *version*))
(def *analyzer* (SnowballAnalyzer. *version* "English"))
(def *optimize-frequency* 1)

(defstruct
    #^{:doc "Structure for clucy indexes."}
    clucy-index :index :optimize-frequency :updates)

;; flag to indicate a default "_content" field should be maintained
(def *content* true)

(defn memory-index
  "Create a new index in RAM."
  []
  (atom (struct-map clucy-index
          :index (RAMDirectory.)
          :optimize-frequency *optimize-frequency*
          :updates 0)))

(defn disk-index
  "Create a new index in a directory on disk."
  [dir-path]
  (atom (struct-map clucy-index
          :index (NIOFSDirectory. (File. dir-path))
          :optimize-frequency *optimize-frequency*
          :updates 0)))

(defn- index-writer
  "Create an IndexWriter."
  [index]
  (IndexWriter. (:index @index)
                *analyzer*
                IndexWriter$MaxFieldLength/UNLIMITED))

(defn- index-reader
  "Create an IndexReader."
  [index]
  (IndexReader/open (:index @index)))


(defn- optimize-index
  "Optimized the provided index if the number of updates matches or
  exceeds the optimize frequency."
  [index]
  (if (<= (:optimize-frequency @index) (:updates @index))
    (with-open [writer (index-writer index)]
      (.optimize writer)
      (swap! index assoc :updates 0))
    index))

(defn- add-field
  "Add a Field to a Document."
  ([document key value]
     (add-field document key value {}))

  ([document key value meta-map]
       (.add document
             (Field. (as-str key) (as-str value)
                     (if (and meta-map (= false (:stored meta-map)))
                       Field$Store/NO
                       Field$Store/YES)
                     (if (and meta-map (= false (:indexed meta-map)))
                       Field$Index/NO
                       Field$Index/ANALYZED)))))

(defn- map-stored
  "Returns a hash-map containing all of the values in the map that
  will be stored in the search index."
  [map-in]
  (merge {}
         (filter (complement nil?)
                 (map (fn [item]
                        (if (or (= nil (meta map-in))
                                (not= false
                                      (:stored ((first item) (meta map-in)))))
                          item)) map-in))))

(defn- concat-values
  "Concatenate all the maps values being stored into a single string."
  [map-in]
  (apply str (interpose " " (vals (map-stored map-in)))))

(defn- map->document
  "Create a Document from a map."
  [map]
  (let [document (Document.)]
    (doseq [[key value] map]
      (add-field document key value (key (meta map))))
    (if *content*
      (add-field document :_content (concat-values map)))
    document))

(defn add
  "Add hash-maps to the search index."
  [index & maps]
  (with-open [writer (index-writer index)]
    (doseq [m maps]
      (swap! index assoc :updates (inc (:updates @index)))
      (.addDocument writer (map->document m))))
  (optimize-index index))

(defn delete
  "Deletes hash-maps from the search index."
  [index & maps]
  (with-open [writer (index-writer index)]
    (doseq [m maps]
      (let [query (BooleanQuery.)]
        (doseq [[key value] m]
          (.add query
                (BooleanClause.
                 (TermQuery. (Term. (.toLowerCase (as-str key))
                                    (.toLowerCase (as-str value))))
                 BooleanClause$Occur/MUST)))
        (.deleteDocuments writer query))
      (swap! index assoc :updates (inc (:updates @index)))))
  (optimize-index index))

(defn- document->map
  "Turn a Document object into a map."
  [document]
  (with-meta
    (-> (into {}
              (for [f (.getFields document)]
                [(keyword (.name f)) (.stringValue f)]))
        (dissoc :_content))
    (-> (into {}
              (for [f (.getFields document)]
                [(keyword (.name f))
                 {:indexed (.isIndexed f)
                  :stored (.isStored f)
                  :tokenized (.isTokenized f)}]))
        (dissoc :_content))))

(defn search
  "Search the supplied index with a query string."
  ([index query max-results]
     (if *content*
       (search index query max-results :_content)
       (throw (Exception. "No default search field specified"))))
  ([index query max-results default-field]
    (with-open [searcher (IndexSearcher. (:index @index))]
      (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
            query  (.parse parser query)
            hits   (.search searcher query max-results)]
        (doall
          (for [hit (.scoreDocs hits)]
            (document->map (.doc searcher (.doc hit)))))))))

(defn search-and-delete
  "Search the supplied index with a query string and then delete all
of the results."
  ([index query]
     (if *content*
       (search-and-delete index query :_content)
       (throw (Exception. "No default search field specified"))))
  ([index query default-field]
    (with-open [writer (index-writer index)]
      (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
            query  (.parse parser query)]
        (.deleteDocuments writer query)
        (swap! index assoc :updates (inc (:updates @index)))))
    (optimize-index index)))


(defn retrieve
  "Retrievs from index document by it's Lucene number.
   Don't forget, that after optimize-index numbers in index may change."
  [index n]
  (with-open [reader (index-reader index)]
    (document->map (.document reader n))))

;; utils ;;

(defn analyze
  "Breaks text into list of terms using specified anayzer"
  ([text] (analyze text *analyzer*))
  ([text analyzer]
     (let [reader (java.io.StringReader. text)
	   tokens (.tokenStream analyzer nil reader)
	   term-attr (.getAttribute tokens TermAttribute)]
       (loop [ret (transient [])]
	 (if (not (.incrementToken tokens))
	   (persistent! ret)
	   (recur (conj! ret (.term term-attr))))))))

(defn count-per-doc
  "Returns map where keys are numbers of (not deleted) documents
   and values are counts of term occurrences in this doc."
  ([index word] (count-per-doc index word :_content))
  ([index word field]
     (with-open [reader (index-reader index)]
       (let [term (Term. (as-str field) word)
	     term-docs (.termDocs reader term)]
	 (loop [ret (transient {})]
	   (if (not (.next term-docs))
	     (persistent! ret)
	     (recur (assoc! ret (.doc term-docs) (.freq term-docs)))))))))

(defn all-doc-numbers
  "Returns list of numbers of all (not deleted) documents."
  [index]
  (with-open [reader (index-reader index)]
    (let [num-docs (.numDocs reader)]
      (loop [i num-docs, ret (list)]
	(if (= i 0)
	  ret
	  (if (.isDeleted reader i)
	    (recur (- i 1) ret)
	    (recur (- i 1) (conj ret i))))))))

(defn all-words
  "Returns list of all words."
  [index]
  (with-open [reader (index-reader index)]
    (let [terms (.terms reader)]
      (loop [ret (transient #{})]
	(if (.next terms)
	  (recur (conj! ret (.text (.term terms))))
	  (seq (persistent! ret)))))))

(defn detect-lang
  "Tries to guess the language the text is written in.
   Result - 2-character language code (e.g. 'en', 'fr', etc.)"
  [text]
  (let [idf (LanguageIdentifier. text)]
    (.getLanguage idf)))

;; test data ;;

(defn init-tests []
  (def idx (memory-index))
  (add idx {:title "William Shakespeare: Dream in a Summer Night"})
  (add idx {:title "Progmatic bookshelf: Programming Clojure"})
  (add idx {:title "Science of a Dream"}))
