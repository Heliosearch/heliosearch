<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:param name="html.stylesheet" />

  <!-- Wraps the content in required divs -->
  <xsl:template name="local.body.wrapper">
    <xsl:param name="doc" select="self::*" />
    <xsl:param name="prev" />
    <xsl:param name="next" />
    <xsl:param name="nav.context" />
    <xsl:param name="content" />

    <ul id="toggle-top">
      <li>
        <div class="panel-top">
          <div class="container"><h1></h1><p></p></div>
        </div>
      </li>
    </ul>

    <header id="header-bg">
      <div class="container" id="top-container">
        <div id="social">
          <div style="text-align: left;">
            <div style="height: 0px; overflow: hidden"></div>
            <span class="wsite-social wsite-social-default">
              <a class="wsite-social-item wsite-social-rss" href="https://plus.google.com/+Sindicetechgplus" target="_blank"></a>
              <a class="wsite-social-item wsite-social-twitter" href="https://twitter.com/SindiceTech" target="_blank"></a>
              <a class="last-child wsite-social-item wsite-social-linkedin" href="http://www.linkedin.com/company/2289136" target="_blank"></a>
            </span>
            <div style="height: 0px; overflow: hidden"></div>
          </div>
        </div>
      </div>

      <div class="wrapper">
        <nav id="menu">
          <ul class="wsite-menu-default">
            <li class="wsite-nav-0" style="position: relative;">
              <a href="http://sirendb.com/" style="position: relative;">Home</a>
            </li>

            <li class="wsite-nav-0" style="position: relative;">
              <a href="http://sirendb.com/product/" style="position: relative;">Product</a>
            </li>

            <li class="wsite-nav-0" style="position: relative;">
              <a href="http://sirendb.com/downloads/" style="position: relative;">Downloads</a>
            </li>

            <li class="wsite-nav-0" style="position: relative;">
              <a href="http://sirendb.com/docs/" style="position: relative;">Docs</a>
            </li>

            <li class="wsite-nav-0" style="position: relative;">
              <a href="http://sirendb.com/company/" style="position: relative;">Company</a>
            </li>

            <li class="wsite-nav-0" id="active" style="position: relative;">
              <a href="http://sirendb.com/blog/" style="position: relative;">Blog</a>
            </li>
          </ul>
        </nav>
      </div>

      <div class="container">
        <div id="logo">
          <h1><span class="wsite-logo"><a href=""><img src="./images/1390314293.png"/></a></span></h1>
        </div>
      </div>
    </header>

    <div class="clearfix" id="content">

      <div class="contentContainer" id="contentContainer">
        <article>
          <!-- include content -->
          <xsl:call-template name="local.body.content">
            <xsl:with-param name="node" select="$doc" />
            <xsl:with-param name="prev" select="$prev" />
            <xsl:with-param name="next" select="$next" />
            <xsl:with-param name="nav.context" select="$nav.context" />
            <xsl:with-param name="content" select="$content" />
          </xsl:call-template>
          <!-- content done -->
        </article>
      </div>
    </div>

    <!-- footer -->
    <footer>
      <div class="stripe-cover" id="footer">
        <div id="flexifooter">
          <div class="wsite-elements wsite-not-footer">
            <div>
              <div class="paragraph" style="text-align:right; padding-right:1em;">
                Copyright 2014 - Sindice LTD
              </div>
            </div>
          </div>
        </div>
      </div>
    </footer>

    <!-- google analytics -->
    <xsl:text disable-output-escaping="yes">
    <![CDATA[
     <script type="text/javascript">
      var _gaq = _gaq || [];
      _gaq.push(['_setAccount', 'UA-32398828-2']);
      _gaq.push(['_trackPageview']);

      (function() {
        var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
        ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
        var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
      })();
     </script>
    ]]>
    </xsl:text>

  </xsl:template>

</xsl:stylesheet>
