import pytest

from dbldatagen import HtmlUtils, SparkSingleton

spark = SparkSingleton.getLocalInstance("unit tests")


class TestHtmlUtils:

    @pytest.mark.parametrize("content",
                             ["""
                                for x in range(10):
                                    print(x)
                             """]
)
    def test_html_format_code(self, content):
        formattedContent = HtmlUtils.formatCodeAsHtml(content)
        assert formattedContent is not None
        assert content in formattedContent

    @pytest.mark.parametrize("content, heading",
                             [("""
                                this is a test 
                                this is another one
                             """, "testing"
                             )])
    def test_html_format_content(self, content, heading):
        formattedContent = HtmlUtils.formatTextAsHtml(content, title=heading)

        assert formattedContent is not None, "formatted output is None"

        assert content in formattedContent, "original content missing"
        assert heading in formattedContent, "heading missing from content"

