<!--
  Generates chunked XHTML documents from DocBook XML source using DocBook XSL
  stylesheets.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:import href="http://docbook.sourceforge.net/release/xsl/current/xhtml/chunk.xsl"/>

  <xsl:import href="common.xsl"/>

  <xsl:import href="html_wrappers.xsl"/>

  <xsl:import href="head.xsl"/>

  <xsl:import href="navigation.xsl"/>

  <xsl:import href="toc.xsl"/>

  <!-- chunking options -->
  <xsl:param name="use.id.as.filename" select="1"/>
  <xsl:param name="chunker.output.encoding"  select="'UTF-8'"/>
  <xsl:param name="chunk.first.sections" select="1"/>
  <xsl:param name="chunk.section.depth" select="1"/>
  <xsl:param name="chunk.quietly" select="0"/>
  <xsl:param name="chunk.toc" select="''"/>
  <xsl:param name="chunk.tocs.and.lots" select="0"/>

  <!-- customise main template to add content wrappers -->
  <xsl:template name="chunk-element-content">
    <xsl:param name="prev"/>
    <xsl:param name="next"/>
    <xsl:param name="nav.context"/>
    <xsl:param name="content">
      <xsl:apply-imports/>
    </xsl:param>

    <xsl:call-template name="user.preroot"/>

    <html>
      <xsl:call-template name="html.head">
        <xsl:with-param name="prev" select="$prev"/>
        <xsl:with-param name="next" select="$next"/>
      </xsl:call-template>

      <body>
        <xsl:call-template name="body.attributes"/>
        <xsl:call-template name="local.body.wrapper">
          <xsl:with-param name="prev" select="$prev" />
          <xsl:with-param name="next" select="$next" />
          <xsl:with-param name="nav.context" select="$nav.context" />
          <xsl:with-param name="content" select="$content" />
        </xsl:call-template>
      </body>
    </html>
    <xsl:value-of select="$chunk.append"/>
  </xsl:template>

  <!-- content to wrap -->
  <xsl:template name="local.body.content">
    <xsl:param name="prev" />
    <xsl:param name="next" />
    <xsl:param name="nav.context"/>
    <xsl:param name="content" />

    <xsl:call-template name="user.header.navigation"/>

    <xsl:call-template name="header.navigation">
      <xsl:with-param name="prev" select="$prev"/>
      <xsl:with-param name="next" select="$next"/>
      <xsl:with-param name="nav.context" select="$nav.context"/>
    </xsl:call-template>

    <xsl:call-template name="user.header.content"/>

    <xsl:copy-of select="$content"/>

    <xsl:call-template name="user.footer.content"/>

    <xsl:call-template name="footer.navigation">
      <xsl:with-param name="prev" select="$prev"/>
      <xsl:with-param name="next" select="$next"/>
      <xsl:with-param name="nav.context" select="$nav.context"/>
    </xsl:call-template>

    <xsl:call-template name="user.footer.navigation"/>
  </xsl:template>

  <!-- Remove the <hr/> separator for the title page -->
  <xsl:template name="book.titlepage.separator"></xsl:template>

</xsl:stylesheet>

