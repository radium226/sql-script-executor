package adrien.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import adrien.struct.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.LineReader;

public class ScriptExecutor {

	final private static Logger LOGGER = LoggerFactory
			.getLogger(ScriptExecutor.class);

	public static enum Option {

		SQL_TERMINATOR(";"), BLOCK_TERMINATOR("."), DEFINE("&"), CONCAT("."), ESCAPE("\\");
		
		private String defaultValue;
		
		private Option(String defaultValue) {
			this.defaultValue = defaultValue;
		}
		
		public String getDefaultValue() {
			return defaultValue;
		}

	}

	private String blockTerminator = Option.BLOCK_TERMINATOR.getDefaultValue();
	private String sqlTerminator = Option.SQL_TERMINATOR.getDefaultValue();
	private String substitutionVariablePrefix = Option.DEFINE.getDefaultValue();
	private String substitutionVariableTerminator = Option.CONCAT.getDefaultValue();
	private String substitutionVariableEscaper = Option.ESCAPE.getDefaultValue();

	private Map<String, String> substitutionVariables = Maps.newHashMap();
	
	private Stack<File> scriptDirectory = new Stack<File>();
	
	private StatementExecutor statementExecutor;

	// Create an instance without setting the script directory.
	public ScriptExecutor(StatementExecutor statementExecutor) {
		super();
		
		this.statementExecutor = statementExecutor;
	}

	protected void pushScriptDirectory(File scriptDirectory) {
		this.scriptDirectory.push(scriptDirectory);
	}
	
	protected File popScriptDirectory() {
		return this.scriptDirectory.pop();
	}
	
	protected File peekScriptDirectory() {
		return this.scriptDirectory.peek();
	}

	// Return the script directory or the working directory if it has not been
	// defined.
	public File getScriptDirectory() {
		File actualScriptDirectory = peekScriptDirectory();
		if (actualScriptDirectory == null) {
			File workingDirectory = getWorkingDirectory();
			LOGGER.warn("Working directory {} used as script directory",
					workingDirectory);
			actualScriptDirectory = workingDirectory;
		}
		return actualScriptDirectory;
	}

	// Return the working directory.
	public File getWorkingDirectory() {
		String workingDirectoryPath = System.getProperty("user.dir");
		File workingDirectory = new File(workingDirectoryPath);
		return workingDirectory;
	}

	// Return the connection to use if defined.
	public Connection getConnection() {

		return null;
	}

	public void executeScript(File scriptFile) throws IOException, SQLException {
		executeScript(scriptFile, new String[0]);
	}

	public void executeScript(InputStream inputStream) throws IOException, SQLException {
		executeScript(inputStream, new String[0]);
	}

	public void executeScript(Reader reader) throws IOException, SQLException {
		executeScript(reader, new String[0]);
	}
	
	public void executeScript(File scriptFile, String[] arguments) throws IOException, SQLException {
		File scriptDirectory = scriptFile.getParentFile();
		pushScriptDirectory(scriptDirectory);
		if (!scriptFile.exists()) {
			LOGGER.warn("Unable to run the {} script because it does not exist", scriptFile);
		} else {
			executeScript(new FileInputStream(scriptFile), arguments);
		}
		popScriptDirectory();
	}

	public void executeScript(InputStream inputStream, String[] arguments) throws IOException,
			SQLException {
		executeScript(new InputStreamReader(inputStream), arguments);
	}

	public void executeScript(Reader reader, String[] arguments) throws IOException, SQLException {
		for (int i = 0; i < arguments.length; i++) {
			substitutionVariables.put(Integer.toString(i + 1), arguments[i]);
		}
		
		LineReader lineReader = new LineReader(reader);

		StringBuilder buffer = null;
		String line = null;
		boolean insideBlock = false;
		boolean insideSQL = false;
		while (true) {
			// We read the line if any.
			line = readLine(lineReader);
			if (line == null) {
				break;
			}

			LOGGER.trace("line={}", line);
			LOGGER.trace("insideBlock={}", insideBlock);

			// We create the SQL buffer if needed.
			if (buffer == null) {
				LOGGER.debug("Creating new SQL buffer");
				buffer = new StringBuilder();
				insideBlock = false;
				insideSQL = false;
			}

			// If line is empty, we go to the next line.
			if (!insideBlock && isEmpty(line)) {
				LOGGER.debug("Empty line encoutered");

				// If the line is a one-line comment.
			} else if (isOneLineComment(line)) {
				LOGGER.debug("One-line comment encoutered ({})", line);

			} else if (!insideSQL && insideBlock && isBlockEnd(line, buffer)) {
				LOGGER.debug("PL/SQL terminator encountered({})", line);
				executeStatement(buffer);
				buffer = null;
				insideBlock = false;

				// If it is the beginning of a PL/SQL block.
			} else if (!insideSQL && !insideBlock && isBlockStart(line, buffer)) {
				LOGGER.debug("It's a block start ({})", line);
				insideBlock = true;

				// If it's an include.
			} else if (!insideSQL && !insideBlock && isInclude(line, buffer)) {
				LOGGER.debug("Include encountered ({})", line);
				File scriptFile = parseInclude(line);
				// TODO: Handle arguments
				executeScript(scriptFile);
				buffer = null;
				insideBlock = false;

			} else if (!insideSQL && !insideBlock && isEXIT(line)) {
				LOGGER.debug("EXIT encountered ({})", line);
				break;

			// If it's a SET. 
			} else if (!insideSQL && !insideBlock && isSET(line, buffer)) {
				LOGGER.debug("SET encountered ({})", line);
				
				Pair<Option, Optional<String>> set = parseSET(line);
				Option option = set.getFirst();
				Optional<String> value = set.getSecond();
				switch (option) {
					case SQL_TERMINATOR:
						setSQLTerminator(value.or(Option.SQL_TERMINATOR.getDefaultValue()));
						break;
	
					case BLOCK_TERMINATOR:
						setBlockTerminator(value.or(Option.BLOCK_TERMINATOR.getDefaultValue()));
						break;
						
					case DEFINE:
						setSubstitutionVariablePrefix(value.or(Option.DEFINE.getDefaultValue()));
						break;
						
					case CONCAT:
						setSubstitutionVariableTerminator(value.or(Option.CONCAT.getDefaultValue()));
						break;
					case ESCAPE:
						setSubstitutionVariableEscaper(value.or(Option.ESCAPE.getDefaultValue()));
					default:
						throw new IllegalStateException();
				}

			
			// If it's a DEFINE. 
			} else if (!insideBlock && isDEFINE(line, buffer)) {
				LOGGER.debug("DEFINE encountered ({})", line);
				Pair<Optional<String>, Optional<String>> define = parseDEFINE(line);
				Optional<String> name = define.getFirst();
				Optional<String> value = define.getSecond();
				if (value.isPresent()) {
					putSubstitutionVariable(name.get(), value.get());
				} else {
					if (name.isPresent()) {
						printSubstitutionVariable(name.get());
					} else {
						printAllSubstitutionVariables();
					}
				}
				
				
			// If it is the end of a SQL statement.
			} else if (!insideBlock && isSQLEnd(line, buffer)) {
				executeStatement(buffer);
				buffer = null;
				insideSQL = false;
				insideBlock = false;

				// Else we fill the SQL buffer.
			} else {
				if (!insideBlock) {
					insideSQL = true;
				}
				
				LOGGER.debug("Appening line to SQL buffer");
				buffer.append(withEOL(line));
			}
		}

		if (!isNullOrEmpty(buffer)) {
			String artifacts = buffer.toString();
			LOGGER.warn("Unexecuted artifacts ({})", artifacts);
		}
	}

	// Read the line from the line reader and trim it.
	protected String readLine(LineReader lineReader) throws IOException {
		String line = lineReader.readLine();
		return line == null ? null : line.trim();
	}

	// Find if the line is empty.
	protected boolean isEmpty(String line) {
		return line.isEmpty();
	}

	// Find if the line is a one-line comment.
	protected boolean isOneLineComment(String line) {
		String[] prefixes = new String[] { "--", "#", "//" };
		String expression = "^" + join(quote(prefixes));
		return matches(expression, line);
	}

	// Append an end-of-line character sequence at the end of the line.
	protected String withEOL(String line) {
		return String.format("%s%n", line);
	}

	// Set the PL/SQL terminator
	protected void setBlockTerminator(String blockTerminator) {
		LOGGER.debug("Changing the block terminator from {} to {}", this.blockTerminator, blockTerminator);
		this.blockTerminator = blockTerminator;
	}

	// Set the PL/SQL terminator
	protected void setSQLTerminator(String sqlTerminator) {
		LOGGER.debug("Changing the SQL terminator from {} to {}", this.sqlTerminator, sqlTerminator);
		this.sqlTerminator = sqlTerminator;
	}

	// Find if it is the end of a PL/SQL statement.
	protected boolean isBlockEnd(String line, StringBuilder buffer) {
		String expression = "^[" + Pattern.quote("/")
				+ Pattern.quote(blockTerminator) + "]$";
		boolean blockEnd = matches(expression, line);
		return blockEnd;
	}

	// Find if it is the begin of a PL/SQL statement.
	protected boolean isBlockStart(String line, StringBuilder buffer) {
		String[] types = new String[] { "FUNCTION", "LIBRARY",
				"PACKAGE( BODY)?", "PROCEDURE", "TRIGGER", "TYPE" };
		String expression = ("(^| )CREATE (OR REPLACE)? " + join(types) + " ")
				.replace(" ", "(\\s|\\n)+");
		LOGGER.trace("regEx={}", expression);

		boolean blockStart = matches(expression, line)
				|| startWithOneOf(line, "BEGIN", "DECLARE");

		LOGGER.debug("blockStart({})={}", line, blockStart);

		if (blockStart) {
			buffer.append(withEOL(line));
		}
		return blockStart;
	}

	// Find if the line end with the SQL terminator.
	protected boolean isSQLEnd(String line, StringBuilder buffer) {
		String[] terminators = new String[] {sqlTerminator, "/"};
		boolean sqlEnd = endWithOneOf(line, terminators);
		
		if (sqlEnd) {
			LOGGER.debug("SQL ending ({})", line);
			buffer.append(line.substring(0, line.length() - 1));
		}
		return sqlEnd;
	}

	// Find if it's the EXIT keyword.
	protected boolean isEXIT(String line) {
		return endWith(line, "EXIT");
	}

	protected boolean isInclude(String line, StringBuilder buffer) {
		boolean include = isIncludeFromScriptDirectory(line)
				|| isIncludeFromWorkingDirectory(line);
		LOGGER.debug("isInclude({}) = {}", line, include);
		return include;
	}

	// Find if it's either @ or START
	protected boolean isIncludeFromWorkingDirectory(String line) {
		return startWithOneOf(line, "START", "@");
	}

	// Find if it's @@.
	protected boolean isIncludeFromScriptDirectory(String line) {
		return startWithOneOf(line, "@@");
	}

	// Return the file to include.
	protected File parseInclude(String line) {
		File folderPath = null;
		if (isIncludeFromScriptDirectory(line)) {
			folderPath = getScriptDirectory();
		} else if (isIncludeFromWorkingDirectory(line)) {
			folderPath = getWorkingDirectory();
		}

		String expression = "^(START(\\s|\\n)+|@@?)(.+)(" + sqlTerminator
				+ ")?$";
		Pattern pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(line);

		String fileName = null;
		if (matcher.matches()) {
			fileName = matcher.group(3);
		}

		File file = new File(folderPath, fileName);
		return file;
	}

	// Find if it's the SET keyword.
	protected boolean isSET(String line, StringBuilder buffer) {
		return startWith(line, "SET");
	}
	
	// Parse the SET keyword
	protected Pair<Option, Optional<String>> parseSET(String line) {
		String expression = "^SET\\s+(\\S+)(\\s+(\\S+))?$";
		Pattern pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(line);
		
		String value = null; 
		Option option = null;
		if (matcher.matches()) {
			String optionName =  matcher.group(1);
			LOGGER.debug("optionName=" + optionName);
			value = Strings.emptyToNull(matcher.group(3));
			if (optionName.equals("SQLTERMINATOR")) {
				option = Option.SQL_TERMINATOR;
			} else if (optionName.equals("BLOCKTERMINATOR")) {
				option = Option.BLOCK_TERMINATOR;
			} else {
				option = Option.valueOf(optionName);
			}
		} else {
			throw new IllegalStateException();
		}

		return new Pair<Option, Optional<String>>(option, Optional.fromNullable(value));
	}

	
	// Find if it's the DEFINE keyword.
	protected boolean isDEFINE(String line, StringBuilder buffer) {
		return startWith(line, "DEFINE");
	}
	
	protected Pair<Optional<String>, Optional<String>> parseDEFINE(String line) {
		String expression = "^DEFINE(\\s+([a-zA-Z1-9_]+)(\\s+=\\s+(.+))?)?$";
		Pattern pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(line);
		Pair<Optional<String>, Optional<String>> define = null; 
		if (matcher.matches()) {
			Optional<String> name = Optional.fromNullable(matcher.group(2));
			Optional<String> value = Optional.fromNullable(substituteVariables(matcher.group(4)));
			define = new Pair<Optional<String>, Optional<String>>(name, value);
		}
		
		return define;
	}
	
	public void executeStatement(StringBuilder buffer) throws SQLException {
		String sql = substituteVariables(buffer.toString().trim());
		//LOGGER.info("SQL=<" + sql + ">");
		statementExecutor.executeStatement(sql);
	}

	protected static boolean isNullOrEmpty(StringBuilder buffer) {
		return buffer == null || buffer.length() == 0;
	}

	protected String[] quote(String[] unquoted) {
		int length = unquoted.length;
		String[] quoted = new String[length];
		for (int i = 0; i < length; i++) {
			quoted[i] = Pattern.quote(unquoted[i]);
		}

		return quoted;
	}

	protected String join(String[] unjoined) {
		String joined = "(" + Joiner.on("|").join(unjoined) + ")";
		return joined;
	}

	protected boolean matches(String expression, String line) {
		Pattern pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(line);
		return matcher.find();
	}

	protected boolean matches(String[] expressions, String line) {
		for (String expression : expressions) {
			if (matches(expression, line)) {
				return matches(expression, line);
			}
		}
		return false;
	}

	protected boolean startWithOneOf(String line, String... words) {
		String expression = "^" + join(quote(words));
		return matches(expression, line);
	}

	protected boolean startWith(String line, String word) {
		return startWithOneOf(line, word);
	}

	protected boolean endWithOneOf(String line, String... words) {
		String expression = join(quote(words)) + "$";
		return matches(expression, line);
	}

	protected boolean endWith(String line, String word) {
		return endWithOneOf(line, word);
	}
	
	protected void setSubstitutionVariablePrefix(String substitutionVariablePrefix) {
		LOGGER.debug("Changing substitution variable prefix from {} to {}", this.substitutionVariablePrefix, substitutionVariablePrefix);
		this.substitutionVariablePrefix = substitutionVariablePrefix;
	}

	protected void putSubstitutionVariable(String name, String value) {
		substitutionVariables.put(name, value);
	}
	
	protected void printSubstitutionVariable(String name) {
		String value = substitutionVariables.get(name);
		System.out.println(String.format("DEFINE %s = \"%s\"", name, value));
	}
	
	protected void printAllSubstitutionVariables() {
		for (String name : substitutionVariables.keySet()) {
			printSubstitutionVariable(name);
		}
	}
	
	protected String substituteVariables(String sql) {
		if (sql == null) return null;
		
		String beginer = substitutionVariablePrefix;
		String escaper = substitutionVariableEscaper;
		String terminator = substitutionVariableTerminator;
		String identifiers = "[a-zA-Z0-9_]";
		String[] enders = new String[] {"'", "\"", "\\s", "\\n", "$"};
		String expression = "(" + Pattern.quote(escaper) + ")?(" + Pattern.quote(beginer) + "(" + identifiers + "+))(" + join(enders) + "|" + Pattern.quote(terminator) + ")";
		LOGGER.debug("expression={}", expression);
		
		Pattern pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sql);
		StringBuilder substituedSQL = new StringBuilder();
		int startOffset = 0;
		while (matcher.find(startOffset)) {
			int sqlPartStartOffset = matcher.start();
			int sqlPartEndOffset = matcher.end();
			
			String previousSQLPart = sql.substring(startOffset, sqlPartStartOffset);
			substituedSQL.append(previousSQLPart);
			
			String currentSQLPart = sql.substring(sqlPartStartOffset, sqlPartEndOffset);
			
			boolean isEscape = Strings.nullToEmpty(matcher.group(1)).equals(escaper);
			String lastCharacter = matcher.group(4);
			boolean isTerminator = Strings.nullToEmpty(lastCharacter).equals(terminator);
			String variableName = matcher.group(3);
			
			if (!isEscape) {
				String variableValue = Strings.nullToEmpty(substitutionVariables.get(variableName));
				if (variableValue.isEmpty()) {
					LOGGER.warn("The substitution variable {} is not defined.", substitutionVariablePrefix + variableName);
				}
				substituedSQL.append(variableValue).append(isTerminator ? "" : lastCharacter);
			} else {
				substituedSQL.append(currentSQLPart);
			}
			
			startOffset = sqlPartEndOffset;
		}
		
		int lastOffset = sql.length();
		substituedSQL.append(sql.substring(startOffset, lastOffset));
		return substituedSQL.toString(); 
	}
	
	protected void setSubstitutionVariableTerminator(String substitutionVariableTerminator) {
		this.substitutionVariableTerminator = substitutionVariableTerminator;
	}
	
	protected boolean isSubstitutionVariablePrefix(String text) {
		return substitutionVariablePrefix.equalsIgnoreCase(text);
	}
	
	protected boolean isSubstitutionVariableTerminator(String text) {
		return substitutionVariableTerminator.equalsIgnoreCase(text);
	}
	
	protected boolean isSubstitutionVariableIdentifier(String character) {
		return Character.isJavaIdentifierStart(character.toCharArray()[0]);
	}
	
	protected String characterAt(String text, int position) {
		return Character.toString(text.charAt(position));
	}
	
	protected void setSubstitutionVariableEscaper(String substitutionVariableEscaper) {
		this.substitutionVariableEscaper = substitutionVariableEscaper;
	}
	
	protected int lastIndexOf(String text, String[] characters) {
		int lastIndex = -1;
		for (String character : characters) {
			lastIndex = Math.max(lastIndex, text.lastIndexOf(character));
		}
		
		return lastIndex;
	}
	
}
