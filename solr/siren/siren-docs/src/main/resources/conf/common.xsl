<!--
  Included in chunked.xsl.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <!-- Admon -->

  <xsl:param name="admon.graphics" select="1"/>
  <xsl:param name="admon.graphics.path">images/icons/</xsl:param>
  <xsl:param name="admon.graphics.extension" select="'.png'"/>
  <xsl:param name="admon.style">
    <xsl:text>margin-left: 0; margin-right: 10%;</xsl:text>
  </xsl:param>
  <xsl:param name="admon.textlabel" select="1"/>

  <!-- Callout -->

  <xsl:param name="callout.graphics" select="0"/>
  <xsl:param name="callout.unicode" select="1"/>
  <xsl:param name="callout.unicode.number.limit" select="10"/>

  <!-- Table -->

  <xsl:param name="html.cellpadding" select="'4px'"/>
  <xsl:param name="html.cellspacing" select="''"/>

  <xsl:param name="table.borders.with.css" select="1"/>
  <xsl:param name="table.cell.border.color" select="'#527bbd'"/>

  <xsl:param name="table.cell.border.style" select="'solid'"/>
  <xsl:param name="table.cell.border.thickness" select="'1px'"/>
  <xsl:param name="table.footnote.number.format" select="'a'"/>
  <xsl:param name="table.footnote.number.symbols" select="''"/>
  <xsl:param name="table.frame.border.color" select="'#527bbd'"/>
  <xsl:param name="table.frame.border.style" select="'solid'"/>
  <xsl:param name="table.frame.border.thickness" select="'3px'"/>
  <xsl:param name="tablecolumns.extension" select="'1'"/>

  <!-- Code Block -->

  <!-- add prettyprint classes to code blocks and wrap them into a <code> elements -->

  <xsl:template match="programlisting">
    <xsl:param name="class">
      <xsl:value-of select="local-name(.)" />
      <xsl:if test="@language != ''"> language-<xsl:value-of select="@language" /></xsl:if>
    </xsl:param>

    <pre class="prettyprint">
      <xsl:call-template name="generate.html.lang"/>
      <xsl:call-template name="generate.html.title"/>
      <code>
        <xsl:apply-templates select="." mode="class.attribute">
          <xsl:with-param name="class" select="$class"/>
        </xsl:apply-templates>
        <xsl:apply-templates/>
      </code>
    </pre>
  </xsl:template>

</xsl:stylesheet>

