(defproject clinch/clucy "0.1.3"
  :description "A Clojure interface to the Lucene search engine"
  :url "http://github/faithlessfriend/clucy"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.apache.lucene/lucene-core "3.0.3"]
		 [org.apache.lucene/lucene-snowball "3.0.3"]
		 [org.apache.lucene/lucene-analyzers "3.0.3"]
		 [org.apache.tika/tika-core "0.7"]]
  :dev-dependencies [[lein-clojars "0.5.0-SNAPSHOT"]
                     [swank-clojure "1.2.1"]]
  :aot [clucy.RplsAnalyzer])
