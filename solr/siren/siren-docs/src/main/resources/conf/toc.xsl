<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <!-- toc -->
  <xsl:param name="generate.section.toc.level"  select="$chunk.section.depth"/>
  <xsl:param name="toc.section.depth"           select="$chunk.section.depth"/>
  <xsl:param name="toc.max.depth"               select="1"/>
  <xsl:param name="generate.toc">
    book      toc
    preface   toc
    chapter   toc
    part      toc
    section   toc
  </xsl:param>

  <!-- Remove part, chapter and section numbering -->
  <xsl:param name="part.autolabel" select="0"/>
  <xsl:param name="chapter.autolabel" select="0"/>
  <xsl:param name="section.autolabel" select="0"/>

  <!-- Disable the TOC title -->
  <xsl:template name="make.toc">
    <xsl:param name="toc-context" select="."/>
    <xsl:param name="nodes" select="/NOT-AN-ELEMENT"/>
    <xsl:if test="$nodes">
      <div class="toc">
        <xsl:element name="{$toc.list.type}">
          <xsl:apply-templates select="$nodes" mode="toc">
            <xsl:with-param name="toc-context" select="$toc-context"/>
          </xsl:apply-templates>
        </xsl:element>
      </div>
    </xsl:if>
  </xsl:template>

  <xsl:template name="division.toc">
    <xsl:param name="toc-context" select="."/>
    <xsl:param name="toc.title.p" select="true()"/>

    <xsl:call-template name="make.toc">
      <xsl:with-param name="toc-context" select="$toc-context"/>
      <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
      <xsl:with-param name="nodes" select="part|reference|preface|chapter|appendix|article|topic|bibliography|glossary|index|refentry|bridgehead[$bridgehead.in.toc != 0]"/>
    </xsl:call-template>
  </xsl:template>

  <!-- generate part-level toc if chapter has no descendants -->
  <xsl:template name="component.toc">
    <xsl:param name="toc-context" select="."/>

    <xsl:variable name="nodes" select="section
                                       |sect1
                                       |simplesect[$simplesect.in.toc != 0]
                                       |refentry
                                       |article|bibliography|glossary
                                       |appendix
                                       |index
                                       |bridgehead[not(@renderas) and $bridgehead.in.toc != 0]
                                       |.//bridgehead[@renderas='sect1' and $bridgehead.in.toc != 0]"/>
    <xsl:choose>
      <xsl:when test="count($nodes) &lt; 2 or $chunk.section.depth = 0">
        <xsl:for-each select="parent::book | parent::part">
          <xsl:call-template name="division.toc">
            <xsl:with-param name="toc-context" select="." />
          </xsl:call-template>
        </xsl:for-each>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="make.toc">
          <xsl:with-param name="toc-context" select="$toc-context"/>
          <xsl:with-param name="nodes" select="$nodes"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <!-- generate chapter-level toc for all top-level sections -->
  <xsl:template name="section.toc">
    <xsl:for-each select="parent::chapter">
      <xsl:call-template name="component.toc">
        <xsl:with-param name="toc-context" select="." />
      </xsl:call-template>
    </xsl:for-each>
  </xsl:template>

  <xsl:template match="preface|chapter|appendix|article" mode="toc">
    <xsl:param name="toc-context" select="."/>

    <xsl:choose>
      <xsl:when test="local-name($toc-context) = 'book'">
        <xsl:call-template name="subtoc">
          <xsl:with-param name="toc-context" select="$toc-context"/>
          <xsl:with-param name="nodes"
                          select="section|sect1|glossary|bibliography|index|bridgehead[$bridgehead.in.toc != 0]"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="subtoc">
          <xsl:with-param name="toc-context" select="$toc-context"/>
          <xsl:with-param name="nodes" select="foo"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>