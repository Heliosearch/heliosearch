<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:param name="html.stylesheet" />

  <xsl:param name="wordpress.dir">..</xsl:param>

  <!-- Wraps the content in required divs -->
  <xsl:template name="local.body.wrapper">
    <xsl:param name="doc" select="self::*" />
    <xsl:param name="prev" />
    <xsl:param name="next" />
    <xsl:param name="nav.context" />
    <xsl:param name="content" />

    <xsl:processing-instruction name="php">
      require('<xsl:value-of select="$wordpress.dir"/>/wp-blog-header.php');

      function assignPageTitle(){
        return "- The SIREn Manual";
      }
      add_filter('wp_title', 'assignPageTitle');

      get_header();
    </xsl:processing-instruction>
    <section id="ww_main_body">
        <div class="ww-container">
            <xsl:processing-instruction name="php">
              get_template_part('includes/ww-main-header/page', 'MainHeader');
            </xsl:processing-instruction>
            <div id="ww_main_content">
                <div class="row-fluid">
                    <div class="span12">
                        <div class="wpb_row vc_row-fluid">
                            <div class="container">
                              <!-- Add empty comment to avoid XSLT to self-close the empty element - Self-closed
                                  element was not properly rendered by Wordpress  -->
                              <div class="vc_span2 wpb_column column_container  "><xsl:comment></xsl:comment></div>
                              <div class="vc_span8 wpb_column column_container  ">
                                <div class="wpb_text_column wpb_content_element ">
                                  <div class="wpb_wrapper">
                                    <div class="manual">
                                      <xsl:call-template name="local.body.content">
                                        <xsl:with-param name="node" select="$doc" />
                                        <xsl:with-param name="prev" select="$prev" />
                                        <xsl:with-param name="next" select="$next" />
                                        <xsl:with-param name="nav.context" select="$nav.context" />
                                        <xsl:with-param name="content" select="$content" />
                                      </xsl:call-template>
                                    </div>
                                  </div>
                                </div>
                              </div>
                              <!-- Add empty comment to avoid SSLT to self-close the empty element - Self-closed
                                   element was not properly rendered by Wordpress  -->
                              <div class="vc_span2 wpb_column column_container  "><xsl:comment></xsl:comment></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </section>

    <xsl:processing-instruction name="php">
      get_footer();
    </xsl:processing-instruction>

    <link href="./css/docbook.css" rel="stylesheet" type="text/css"/>
    <link href="./css/prism.css" rel="stylesheet" type="text/css"/>
    <script src="./js/prism.js" type="text/javascript"></script>
    <script src="./js/scrolltopcontrol.js" type="text/javascript"></script>

  </xsl:template>

</xsl:stylesheet>
