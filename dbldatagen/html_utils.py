# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `HtmlUtils` classes and utility functions
"""

from dbldatagen.utils import system_time_millis


class HtmlUtils:
    """
    Utility class for formatting code as HTML and other notebook-related formatting.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def formatCodeAsHtml(codeText: str) -> str:
        """
        Formats the input code as HTML suitable for use with a notebook's ``displayHTML`` command.

        This method wraps the input code with an html section using ``pre`` and ``code`` tags. It adds a *Copy Text to
        Clipboard* button which allows users to easily copy the code to the clipboard.

        Code is not reformatted. Supplied code should be preformatted into lines.

        :param codeText: Input code as a string
        :return: Formatted code as an HTML string

        .. note::
            As the notebook environment uses IFrames in rendering html within ``displayHtml``, it cannot use
            the newer ``navigator`` based functionality as this is blocked for cross domain IFrames by default.

        """
        current_ts = system_time_millis()

        return f"""
            <h3>Generated Code</h3>
            <div style="outline: 1px dashed blue;"><p ><pre><code id="generated_code_{current_ts}">
              {codeText}
            </code></pre></p></br>
            </div>
            <p><button type="button" onclick="dbldatagen_copy_code_to_clipboard()">Copy code to clipboard!</button> </p>
            <script>
            function dbldatagen_copy_code_to_clipboard() {{
               try {{
                 var r = document.createRange();
                 r.selectNode(document.getElementById("generated_code_{current_ts}"));
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

    @staticmethod
    def formatTextAsHtml(textContent: str, title: str = "Output") -> str:
        """
        Formats the input text as HTML suitable for use with a notebook's ``displayHTML`` command. This wraps the text
        content with HTML formatting blocks and adds a section title.

        :param textContent: Input text to be wrapped in an HTML section
        :param title: Section title (default `"Output"`)
        :return: Text section as an HTML string
        """
        current_ts = system_time_millis()
        return f"""
            <h3>{title}</h3>
            <div style="outline: 1px dashed blue;"><p ><pre id="generated_content_{current_ts}">
              {textContent}
            </pre></p></br>
            </div>
            <p><button type="button" onclick="dbldatagen_copy_to_clipboard()">Copy output to clipboard!</button></p>
            <script>
            function dbldatagen_copy_to_clipboard() {{
               try {{
                 var r = document.createRange();
                 r.selectNode(document.getElementById("generated_content_{current_ts}"));
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
