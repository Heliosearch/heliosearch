
# cd ..; ant compile compile-test 
# to ensure all the needed classes are compiled
# also make sure the native lib is in the "example" directory
S=..

JSEP=":"
OS=`uname`
case $OS in
  CYGWIN*)
    JSEP=";"
  ;;
esac

export PATH=`pwd`:$PATH
SLF4J=$S/solrj/lib/slf4j-api-1.7.6.jar

java -Djava.library.path=$S/example -cp "$S/build/solr-core/classes/java/${JSEP}$S/build/solr-solrj/classes/java${JSEP}$S/build/lucene-libs/*${JSEP}$S/build/solr-core/classes/test/${JSEP}${SLF4J}${JSEP}./" org.apache.solr.search.TestDocSetPerf $*

# example args...  compare int to native int
# 100 INT
# 100 NINT

