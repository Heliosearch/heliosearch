<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <!-- breadcrumbs -->
  <xsl:template name="breadcrumbs">

    <xsl:param name="this.node" select="."/>

    <div class="breadcrumbs">
      <span class="breadcrumb-link">
        <a href="index.html">SIREn Manual</a>
      </span>

    <xsl:if test="name(.) != 'book'">
        <xsl:text> &gt; </xsl:text>
        <xsl:for-each select="$this.node/ancestor::*[parent::*]">
          <span class="breadcrumb-link">
            <a>
              <xsl:attribute name="href">
                <xsl:call-template name="href.target">
                  <xsl:with-param name="object" select="."/>
                  <xsl:with-param name="context" select="$this.node"/>
                </xsl:call-template>
              </xsl:attribute>
              <xsl:apply-templates select="." mode="title.markup"/>
            </a>
          </span>
          <xsl:text> &gt; </xsl:text>
        </xsl:for-each>
        <!-- And display the current node, but not as a link -->
        <span class="breadcrumb-node">
          <xsl:apply-templates select="$this.node" mode="title.markup"/>
        </span>
    </xsl:if>

    </div>
  </xsl:template>

  <!-- navigation -->
  <xsl:template name="header.navigation">
    <xsl:param name="prev" />
    <xsl:param name="next" />
    <xsl:param name="nav.context"/>
    <xsl:call-template name="breadcrumbs"/>
    <xsl:call-template name="custom.navigation">
      <xsl:with-param name="nav.class"   select="'navheader'" />
      <xsl:with-param name="prev"        select="$prev" />
      <xsl:with-param name="next"        select="$next" />
      <xsl:with-param name="nav.context" select="$nav.context" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="footer.navigation">
    <xsl:param name="prev" />
    <xsl:param name="next" />
    <xsl:param name="nav.context"/>
    <xsl:call-template name="custom.navigation">
      <xsl:with-param name="nav.class"   select="'navfooter'" />
      <xsl:with-param name="prev"        select="$prev" />
      <xsl:with-param name="next"        select="$next" />
      <xsl:with-param name="nav.context" select="$nav.context" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="custom.navigation">
    <xsl:param name="prev" select="/foo"/>
    <xsl:param name="next" select="/foo"/>
    <xsl:param name="nav.class"  />
    <xsl:param name="nav.context"/>

    <xsl:variable name="row" select="count($prev) &gt; 0
                                      or count($next) &gt; 0"/>
    <xsl:variable name="home" select="/*[1]"/>

    <div>
      <xsl:attribute name="class">
        <xsl:value-of select="$nav.class" />
      </xsl:attribute>
      <xsl:if test="$row">
        <span class="prev">
          <xsl:if test="count($prev)>0 and generate-id($home) != generate-id($prev)">
            <a>
              <xsl:attribute name="href">
                <xsl:call-template name="href.target">
                  <xsl:with-param name="object" select="$prev"/>
                </xsl:call-template>
              </xsl:attribute>
              &#171;&#160;
              <xsl:apply-templates select="$prev" mode="object.title.markup"/>
            </a>
          </xsl:if>
          &#160;
        </span>
        <span class="next">
          &#160;
          <xsl:if test="count($next)>0">
            <a>
              <xsl:attribute name="href">
                <xsl:call-template name="href.target">
                  <xsl:with-param name="object" select="$next"/>
                </xsl:call-template>
              </xsl:attribute>
              <xsl:apply-templates select="$next" mode="object.title.markup"/>
              &#160;&#187;
            </a>
          </xsl:if>
        </span>
      </xsl:if>
    </div>
  </xsl:template>

  <!-- remove table/part/chapter/section titles -->
  <xsl:param name="local.l10n.xml" select="document('')"/>
  <l:i18n xmlns:l="http://docbook.sourceforge.net/xmlns/l10n/1.0">
    <l:l10n language="en">
      <l:context name="title">
        <l:template name="table" text="%t"/>
        <l:template name="part"    text="%t"/>
        <l:template name="chapter" text="%t"/>
        <l:template name="section" text="%t"/>
      </l:context>
      <l:context name="title-unnumbered">
        <l:template name="table" text="%t"/>
        <l:template name="part"    text="%t"/>
        <l:template name="chapter" text="%t"/>
        <l:template name="section" text="%t"/>
      </l:context>
      <l:context name="title-numbered">
        <l:template name="table" text="%t"/>
        <l:template name="part"    text="%t"/>
        <l:template name="chapter" text="%t"/>
        <l:template name="section" text="%t"/>
      </l:context>
      <l:context name="xref">
        <l:template name="table" text="%t"/>
        <l:template name="part"    text="%t"/>
        <l:template name="chapter" text="%t"/>
        <l:template name="section" text="%t"/>
      </l:context>
      <l:context name="xref-number">
        <l:template name="table" text="%t"/>
        <l:template name="part"    text="%t"/>
        <l:template name="chapter" text="%t"/>
        <l:template name="section" text="%t"/>
      </l:context>
      <l:context name="xref-number-and-title">
        <l:template name="table" text="%t"/>
        <l:template name="part"    text="%t"/>
        <l:template name="chapter" text="%t"/>
        <l:template name="section" text="%t"/>
      </l:context>
    </l:l10n>
  </l:i18n>

</xsl:stylesheet>