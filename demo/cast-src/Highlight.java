import app.softnetwork.elastic.client.repl.ReplHighlighter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Renders each stdin line through the REPL's own JLine Highlighter and prints its ANSI form.
 *  One output line per input line; the escape codes are the product's, not ours. */
public class Highlight {
  public static void main(String[] args) throws Exception {
    ReplHighlighter h = new ReplHighlighter();
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = in.readLine()) != null) {
      sb.append(h.highlight(null, line).toAnsi()).append('\n');
    }
    System.out.print(sb);
  }
}
