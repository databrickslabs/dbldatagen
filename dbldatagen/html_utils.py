# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `HtmlUtils` classes and utility functions
"""

from .utils import system_time_millis


class HtmlUtils:
    """ Utility class for formatting code as HTML and other notebook related formatting

    """

    def __init__(self):
        pass

    @classmethod
    def formatCodeAsHtml(cls, codeText):
        """ Formats supplied code as Html suitable for use with notebook ``displayHTML``

        :param codeText: Code to be wrapped in html section
        :return: Html string

        This will wrap the code with a html section using html ``pre`` and ``code`` tags.

        It adds a copy text to clipboard button to enable users to easily copy the code to the clipboard.

        It does not reformat code so supplied code should be preformatted into lines.

        .. note::
            As the notebook environment uses IFrames in rendering html within ``displayHtml``, it cannot use
            the newer ``navigator`` based functionality as this is blocked for cross domain IFrames by default.

        """
        ts = system_time_millis()

        formattedCode = f"""
            <h3>Generated Code</h3>
            <div style="outline: 1px dashed blue;"><p ><pre><code id="generated_code_{ts}"> 
              {codeText}
            </code></pre></p></br>
            </div>
            <p><button type="button" onclick="dbldatagen_copy_code_to_clipboard()">Copy code to clipboard!</button> </p>
            <script>
            function dbldatagen_copy_code_to_clipboard() {{
               try {{
                 var r = document.createRange();
                 r.selectNode(document.getElementById("generated_code_{ts}"));
                 window.getSelection().removeAllRanges();
                 window.getSelection().addRange(r);
                 document.execCommand('copy');
                 window.getSelection().removeAllRanges();
               }}
               catch {{
                 console.error("copy to clipboard failed")
               }}
            }}
        </script>
        """

        return formattedCode

    @classmethod
    def formatTextAsHtml(cls, textContent, title="Output"):
        """ Formats supplied text as Html suitable for use with notebook ``displayHTML``

        :param textContent: Text to be wrapped in html section
        :param title: Title text to be used
        :return: Html string

        This will wrap the text content with with Html formatting

        """
        ts = system_time_millis()
        formattedContent = f"""
            <h3>{title}</h3>
            <div style="outline: 1px dashed blue;"><p ><pre id="generated_content_{ts}"> 
              {textContent}
            </pre></p></br>
            </div>
            <p><button type="button" onclick="dbldatagen_copy_to_clipboard()">Copy output to clipboard!</button></p>
            <script>
            function dbldatagen_copy_to_clipboard() {{
               try {{
                 var r = document.createRange();
                 r.selectNode(document.getElementById("generated_content_{ts}"));
                 window.getSelection().removeAllRanges();
                 window.getSelection().addRange(r);
                 document.execCommand('copy');
                 window.getSelection().removeAllRanges();
               }}
               catch {{
                 console.error("copy to clipboard failed")
               }}
            }}
        </script>
        """

        return formattedContent
