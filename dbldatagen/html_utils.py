# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `HtmlUtils` classes and utility functions
"""


class HtmlUtils:
    """ Utility class for formatting code as HTML and other notebook related formatting

    """

    def __init__(self):
        pass

    @classmethod
    def formatCodeAsHtml(cls, code_text):
        formattedCode = f"""
            <h3>Generated Code</h3>
            <div style="outline: 1px dashed blue;"><p ><pre><code id="generated_code"> 
              {code_text}
            </code></pre></p></br>
            </div>
            <p><button type="button" onclick="dbldatagen_copy_code_to_clipboard()">Copy code to clipboard!</button> </p>
            <script>
            function dbldatagen_copy_code_to_clipboard() {{
               try {{
                 var r = document.createRange();
                 r.selectNode(document.getElementById("generated_code"));
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
