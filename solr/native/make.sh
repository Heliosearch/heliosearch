
echo "Make sure you do an 'ant compile' so the lucene/solr class files are generated first"

#
#production
DEBUG="-DNDEBUG"

#
#debugging
#DEBUG="-g"


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
    SHORT_OS=Mac
    JNI_INC="-I/System/Library/Frameworks/JavaVM.framework/Headers"
  ;;
  Linux)
    SHORT_OS=Linux
    JNI_INC="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
  ;;
  CYGWIN*)
    SHORT_OS=Windows
    JSEP=";"
    JNI_INC="-I$JAVA_HOME/include -I$JAVA_HOME/include/win32"
    GPP="/usr/bin/x86_64-w64-mingw32-g++.exe"
    CFLAGS="-static-libstdc++ -static-libgcc -D_JNI_IMPLEMENTATION_ -Wl,--kill-at -Wl,--enable-auto-image-base -Wl,--add-stdcall-alias -Wl,--enable-auto-import"
  ;;
  *)
    echo "Unknown OS $OS"
    exit 1
  ;;
esac

OUT=libHS_${SHORT_OS}.so

MYPATH=org/apache/solr/core/${CLASS}.java
FULLCLASS=org.apache.solr.core.${CLASS}

LUSOLR=../..
SOLR=..
LUCENE=$LUSOLR/lucene
CLASSES="$SOLR/build/solr-core/classes/java${JSEP}$SOLR/build/solr-solrj/classes/java${JSEP}$LUCENE/build/core/classes/java"

javah -d $BUILD/inc -force -classpath ${CLASSES} ${FULLCLASS}
javah -d $BUILD/inc -force -classpath ${CLASSES} org.apache.solr.search.SortedIntDocSetNative
javah -d $BUILD/inc -force -classpath ${CLASSES} org.apache.solr.search.BitDocSetNative
javah -d $BUILD/inc -force -classpath ${CLASSES} org.apache.solr.search.facet.SimpleFacets

CPPFILES="$CLASS.cpp docset.cpp facet.cpp"
INC="$JNI_INC -I$BUILD/inc"
$GPP $DEBUG $OPT -Wall $CFLAGS $INC -shared -fPIC $CPPFILES -o $BUILD/$OUT
#$GPP -S $DEBUG $OPT -Wall $CFLAGS $INC -shared -fPIC docset.cpp

$GPP $OPT -Wall $CFLAGS $INC         -fPIC $CPPFILES test.cpp -o $BUILD/test.exe
# $GPP -S $OPT -Wall $CFLAGS $INC    -fPIC $CPPFILES test.cpp 

mkdir -p $SOLR/example/native/
cp $BUILD/$OUT $SOLR/example/native/ 

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.
#java $FULLCLASS
