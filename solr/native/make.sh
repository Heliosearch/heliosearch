
echo "Make sure you do an 'ant compile' so the lucene/solr class files are generated first"

GPP=g++
which g++-4.8 > /dev/null 2>&1
if [ $? == 0 ]; then
  GPP=g++-4.8
fi

#separator for java paths... : for unix, ; for windows
JSEP=":"

CLASS=HS
LIBNAME=$CLASS

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
CLASSES="$SOLR/build/solr-core/classes/java${JSEP}$LUCENE/build/core/classes/java"

javah -force -classpath ${CLASSES} ${FULLCLASS}
javah -force -classpath ${CLASSES} org.apache.solr.search.SortedIntDocSetNative


$GPP -O6 -Wall $CFLAGS $JNI_INC -shared -fPIC $CLASS.cpp docset.cpp -o $OUT

cp $OUT $LUSOLR/solr/example

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.
#java $FULLCLASS
