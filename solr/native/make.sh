
echo "Make sure you do an 'ant compile' so the lucene/solr class files are generated first"
echo "NOTE: running the example on linux requires the CWD to be in java.library.path"
echo "example: java -Djava.library.path=. -jar start.jar"

BUILD=./build

mkdir -p $BUILD/inc

GPP=g++
which g++-4.8 > /dev/null 2>&1
if [ $? == 0 ]; then
  GPP=g++-4.8
fi

#separator for java paths... : for unix, ; for windows
JSEP=":"

CLASS=HS
LIBNAME=$CLASS

OPT="-m64 -mtune=corei7 -O6 -msse -msse2 -msse3 -mfpmath=sse"

OS=`uname`
case $OS in
  Darwin)
    OUT=lib${LIBNAME}.jnilib
    JNI_INC="-I/System/Library/Frameworks/JavaVM.framework/Headers"
    LIB_EXT=jnilib
  ;;
  Linux)
    OUT=lib${LIBNAME}.so
    JNI_INC="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
    LIB_EXT=so
  ;;
  CYGWIN*)
    OUT=${LIBNAME}.dll
    JSEP=";"
    JNI_INC="-I$JAVA_HOME/include -I$JAVA_HOME/include/win32"
    LIB_EXT=dll
    GPP="/usr/bin/x86_64-w64-mingw32-g++.exe"
    CFLAGS="-static-libstdc++ -static-libgcc -D_JNI_IMPLEMENTATION_ -Wl,--kill-at -Wl,--enable-auto-image-base -Wl,--add-stdcall-alias -Wl,--enable-auto-import"
  ;;
  *)
    echo "Unknown OS $OS"
    exit 1
  ;;
esac

MYPATH=org/apache/solr/core/${CLASS}.java
FULLCLASS=org.apache.solr.core.${CLASS}

LUSOLR=../..
SOLR=..
LUCENE=$LUSOLR/lucene
CLASSES="$SOLR/build/solr-core/classes/java${JSEP}$SOLR/build/solr-solrj/classes/java${JSEP}$LUCENE/build/core/classes/java"

javah -d $BUILD/inc -force -classpath ${CLASSES} ${FULLCLASS}
javah -d $BUILD/inc -force -classpath ${CLASSES} org.apache.solr.search.SortedIntDocSetNative
javah -d $BUILD/inc -force -classpath ${CLASSES} org.apache.solr.search.facet.SimpleFacets

CPPFILES="$CLASS.cpp docset.cpp facet.cpp"
INC="$JNI_INC -I$BUILD/inc"
$GPP $OPT -Wall $CFLAGS $INC -shared -fPIC $CPPFILES -o $BUILD/$OUT

$GPP $OPT -Wall $CFLAGS $INC         -fPIC $CPPFILES test.cpp -o $BUILD/test.exe
# $GPP -S $OPT -Wall $CFLAGS $INC    -fPIC $CPPFILES test.cpp 

cp $BUILD/$OUT $SOLR/example/        #for the server
cp $BUILD/$OUT $SOLR/build/          #for tests
cp $BUILD/$OUT $LUSOLR/              #for intellij (TODO: get it to look in build or example)

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.
#java $FULLCLASS
