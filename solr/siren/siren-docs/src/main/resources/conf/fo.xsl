<!--
  Generates single FO document from DocBook XML source using DocBook XSL
  stylesheets.
-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format">
  <xsl:import href="http://docbook.sourceforge.net/release/xsl/current/fo/docbook.xsl"/>

  <xsl:param name="fop1.extensions" select="1" />
  <xsl:param name="variablelist.as.blocks" select="1" />

  <xsl:param name="paper.type" select="'A4'"/>

  <!-- Text -->

  <xsl:param name="hyphenate">false</xsl:param>
  <xsl:param name="alignment">justify</xsl:param>

  <!-- Body Font -->

  <xsl:param name="body.font.family" select="'FreeSans'"/>
  <xsl:param name="body.font.master">12</xsl:param>
  <xsl:param name="body.font.size">
    <xsl:value-of select="$body.font.master"/><xsl:text>pt</xsl:text>
  </xsl:param>

  <xsl:param name="title.font.family" select="'FreeSans'"/>

  <!-- Monospace Font - USe DejaVuSansMono as it has wider support for symbols characters -->

  <xsl:param name="monospace.font.family" select="'DejaVuSansMono'"/>

  <xsl:attribute-set name="monospace.properties">
    <xsl:attribute name="font-size">10pt</xsl:attribute>
  </xsl:attribute-set>

  <xsl:template match="row/entry/simpara/literal">
    <fo:inline hyphenate="true" xsl:use-attribute-sets="monospace.properties">
      <xsl:call-template name="intersperse-with-zero-spaces">
        <xsl:with-param name="str" select="text()"/>
      </xsl:call-template>
    </fo:inline>
  </xsl:template>

  <xsl:attribute-set name="monospace.verbatim.properties" use-attribute-sets="verbatim.properties">
    <xsl:attribute name="font-size">8pt</xsl:attribute>
    <xsl:attribute name="line-height">11pt</xsl:attribute>
    <xsl:attribute name="background-color">#F0F0F0</xsl:attribute>
  </xsl:attribute-set>

  <!-- color for links -->

  <xsl:attribute-set name="xref.properties">
    <xsl:attribute name="color">
      <xsl:choose>
        <xsl:when test="self::ulink">blue</xsl:when>
        <xsl:otherwise>inherit</xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="xref.properties">
    <xsl:attribute name="color">blue</xsl:attribute>
  </xsl:attribute-set>

  <!-- Body Margin -->

  <xsl:param name="body.margin.bottom" select="'0.5in'"/>
  <xsl:param name="body.margin.top" select="'0.5in'"/>
  <xsl:param name="bridgehead.in.toc" select="0"/>

  <!-- Line break -->

  <xsl:template match="processing-instruction('asciidoc-br')">
    <fo:block/>
  </xsl:template>

  <!-- Horizontal ruler -->

  <xsl:template match="processing-instruction('asciidoc-hr')">
    <fo:block space-after="1em">
      <fo:leader leader-pattern="rule" rule-thickness="0.5pt"  rule-style="solid" leader-length.minimum="100%"/>
    </fo:block>
  </xsl:template>

  <!-- Hard page break -->

  <xsl:template match="processing-instruction('asciidoc-pagebreak')">
    <fo:block break-after='page'/>
  </xsl:template>

  <!-- Sets title to body text indent -->

  <xsl:param name="body.start.indent">
    <xsl:choose>
      <xsl:when test="$fop.extensions != 0">0pt</xsl:when>
      <xsl:when test="$passivetex.extensions != 0">0pt</xsl:when>
      <xsl:otherwise>1pc</xsl:otherwise>
    </xsl:choose>
  </xsl:param>
  <xsl:param name="title.margin.left">
    <xsl:choose>
      <xsl:when test="$fop.extensions != 0">-1pc</xsl:when>
      <xsl:when test="$passivetex.extensions != 0">0pt</xsl:when>
      <xsl:otherwise>0pt</xsl:otherwise>
    </xsl:choose>
  </xsl:param>
  <xsl:param name="page.margin.bottom" select="'0.25in'"/>
  <xsl:param name="page.margin.inner">
    <xsl:choose>
      <xsl:when test="$double.sided != 0">0.75in</xsl:when>
      <xsl:otherwise>0.75in</xsl:otherwise>
    </xsl:choose>
  </xsl:param>
  <xsl:param name="page.margin.outer">
    <xsl:choose>
      <xsl:when test="$double.sided != 0">0.5in</xsl:when>
      <xsl:otherwise>0.5in</xsl:otherwise>
    </xsl:choose>
  </xsl:param>

  <xsl:param name="page.margin.top" select="'0.5in'"/>
  <xsl:param name="page.orientation" select="'portrait'"/>
  <xsl:param name="page.width">
    <xsl:choose>
      <xsl:when test="$page.orientation = 'portrait'">
        <xsl:value-of select="$page.width.portrait"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$page.height.portrait"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:param>

  <xsl:attribute-set name="admonition.title.properties">
    <xsl:attribute name="font-size">14pt</xsl:attribute>
    <xsl:attribute name="font-weight">bold</xsl:attribute>
    <xsl:attribute name="hyphenate">false</xsl:attribute>
    <xsl:attribute name="keep-with-next.within-column">always</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="sidebar.properties" use-attribute-sets="formal.object.properties">
    <xsl:attribute name="border-style">solid</xsl:attribute>
    <xsl:attribute name="border-width">1pt</xsl:attribute>
    <xsl:attribute name="border-color">silver</xsl:attribute>
    <xsl:attribute name="background-color">#ffffee</xsl:attribute>
    <xsl:attribute name="padding-left">12pt</xsl:attribute>
    <xsl:attribute name="padding-right">12pt</xsl:attribute>
    <xsl:attribute name="padding-top">6pt</xsl:attribute>
    <xsl:attribute name="padding-bottom">6pt</xsl:attribute>
    <xsl:attribute name="margin-left">0pt</xsl:attribute>
    <xsl:attribute name="margin-right">12pt</xsl:attribute>
    <xsl:attribute name="margin-top">6pt</xsl:attribute>
    <xsl:attribute name="margin-bottom">6pt</xsl:attribute>
  </xsl:attribute-set>

  <!-- Only shade programlisting and screen verbatim elements -->
  <xsl:param name="shade.verbatim" select="1"/>
  <xsl:attribute-set name="shade.verbatim.style">
    <xsl:attribute name="background-color">
      <xsl:choose>
        <xsl:when test="self::programlisting|self::screen">#E0E0E0</xsl:when>
        <xsl:otherwise>inherit</xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
  </xsl:attribute-set>

  <!--
    Force XSL Stylesheets 1.72 default table breaks to be the same as the current
    version (1.74) default which (for tables) is keep-together="auto".
  -->
  <xsl:attribute-set name="table.properties">
    <xsl:attribute name="keep-together.within-column">auto</xsl:attribute>
  </xsl:attribute-set>

  <!-- Admon, Callout, Navigation Graphics -->

  <xsl:param name="admon.graphics" select="1"></xsl:param>
  <xsl:param name="admon.textlabel" select="1"></xsl:param>
  <xsl:param name="admon.graphics.extension">.svg</xsl:param>
  <xsl:param name="admon.graphics.path">images/icons/admon/</xsl:param>

  <xsl:param name="callout.graphics" select="1"></xsl:param>
  <xsl:param name="callout.graphics.extension">.svg</xsl:param>

  <xsl:param name="navig.graphics" select="0"></xsl:param>

  <!-- TOC -->

  <xsl:param name="toc.section.depth" select="1"></xsl:param>
  <xsl:param name="toc.max.depth" select="2"></xsl:param>

  <xsl:param name="generate.section.toc.level" select="0"></xsl:param>

  <!-- turned off due to https://issues.apache.org/bugzilla/show_bug.cgi?id=37579 -->
  <xsl:param name="ulink.footnotes" select="0"></xsl:param>
  <xsl:param name="ulink.show" select="1"></xsl:param>

  <xsl:attribute-set name="normal.para.spacing">
    <xsl:attribute name="space-before.optimum">0em</xsl:attribute>
    <xsl:attribute name="space-before.minimum">0em</xsl:attribute>
    <xsl:attribute name="space-before.maximum">0em</xsl:attribute>
    <xsl:attribute name="space-after.optimum">0.5em</xsl:attribute>
    <xsl:attribute name="space-after.minimum">0.4em</xsl:attribute>
    <xsl:attribute name="space-after.maximum">0.6em</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="section.title.properties">
    <xsl:attribute name="space-before.minimum">0.8em</xsl:attribute>
    <xsl:attribute name="space-before.optimum">1.0em</xsl:attribute>
    <xsl:attribute name="space-before.maximum">1.2em</xsl:attribute>
    <xsl:attribute name="space-after.optimum">0.3em</xsl:attribute>
    <xsl:attribute name="space-after.minimum">0.2em</xsl:attribute>
    <xsl:attribute name="space-after.maximum">0.4em</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="list.item.spacing">
    <xsl:attribute name="space-before.optimum">0.20em</xsl:attribute>
    <xsl:attribute name="space-before.minimum">0.15em</xsl:attribute>
    <xsl:attribute name="space-before.maximum">0.25em</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="variablelist.term.properties">
    <xsl:attribute name="space-before.optimum">0.20em</xsl:attribute>
    <xsl:attribute name="space-before.minimum">0.15em</xsl:attribute>
    <xsl:attribute name="space-before.maximum">0.25em</xsl:attribute>
  </xsl:attribute-set>

  <xsl:param name="header.column.widths">1 10 1</xsl:param>

  <xsl:attribute-set name="formal.title.properties" use-attribute-sets="normal.para.spacing">
    <xsl:attribute name="font-size">12pt</xsl:attribute>
    <xsl:attribute name="font-weight">normal</xsl:attribute>
    <xsl:attribute name="font-style">italic</xsl:attribute>
    <xsl:attribute name="hyphenate">false</xsl:attribute>
    <xsl:attribute name="space-before.minimum">0.8em</xsl:attribute>
    <xsl:attribute name="space-before.optimum">1.0em</xsl:attribute>
    <xsl:attribute name="space-before.maximum">1.2em</xsl:attribute>
    <xsl:attribute name="space-after.minimum">0.10em</xsl:attribute>
    <xsl:attribute name="space-after.optimum">0.15em</xsl:attribute>
    <xsl:attribute name="space-after.maximum">0.20em</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="formal.object.properties">
    <xsl:attribute name="space-before.minimum">0em</xsl:attribute>
    <xsl:attribute name="space-before.optimum">0em</xsl:attribute>
    <xsl:attribute name="space-before.maximum">0em</xsl:attribute>
    <xsl:attribute name="space-after.optimum">0.5em</xsl:attribute>
    <xsl:attribute name="space-after.minimum">0.4em</xsl:attribute>
    <xsl:attribute name="space-after.maximum">0.6em</xsl:attribute>
    <xsl:attribute name="keep-together.within-column">always</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="section.title.level1.properties">
    <xsl:attribute name="font-size">18pt</xsl:attribute>
    <xsl:attribute name="space-after.optimum">0.15em</xsl:attribute>
    <xsl:attribute name="space-after.minimum">0.10em</xsl:attribute>
    <xsl:attribute name="space-after.maximum">0.20em</xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level2.properties">
    <xsl:attribute name="font-size">14pt</xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level3.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level4.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
    <xsl:attribute name="font-weight">normal</xsl:attribute>
    <xsl:attribute name="font-style">italic</xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level5.properties">
    <xsl:attribute name="font-size">10pt</xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level6.properties">
    <xsl:attribute name="font-size">10pt</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="section.level1.properties">
    <xsl:attribute name="break-before">page</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="component.title.properties">
    <xsl:attribute name="font-size">18pt</xsl:attribute>
    <xsl:attribute name="space-after.optimum">1.0em</xsl:attribute>
    <xsl:attribute name="space-after.minimum">1.0em</xsl:attribute>
    <xsl:attribute name="space-after.maximum">1.0em</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="informalfigure.properties">
    <xsl:attribute name="text-align">center</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="figure.properties">
    <xsl:attribute name="text-align">center</xsl:attribute>
  </xsl:attribute-set>

</xsl:stylesheet>

