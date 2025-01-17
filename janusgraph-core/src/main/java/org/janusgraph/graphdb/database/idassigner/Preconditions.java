package org.janusgraph.graphdb.database.idassigner;

import com.google.common.annotations.GwtCompatible;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Static convenience methods that help a method or constructor check whether it was invoked
 * correctly (that is, whether its <i>preconditions</i> were met).
 *
 * <p>If the precondition is not met, the {@code Preconditions} method throws an unchecked exception
 * of a specified type, which helps the method in which the exception was thrown communicate that
 * its caller has made a mistake. This allows constructs such as
 *
 * <pre>{@code
 * public static double sqrt(double value) {
 *   if (value < 0) {
 *     throw new IllegalArgumentException("input is negative: " + value);
 *   }
 *   // calculate square root
 * }
 * }</pre>
 *
 * <p>to be replaced with the more compact
 *
 * <pre>{@code
 * public static double sqrt(double value) {
 *   checkArgument(value >= 0, "input is negative: %s", value);
 *   // calculate square root
 * }
 * }</pre>
 *
 * <p>so that a hypothetical bad caller of this method, such as:
 *
 * <pre>{@code
 * void exampleBadCaller() {
 *   double d = sqrt(-1.0);
 * }
 * }</pre>
 *
 * <p>would be flagged as having called {@code sqrt()} with an illegal argument.
 *
 * <h3>Performance</h3>
 *
 * <p>Avoid passing message arguments that are expensive to compute; your code will always compute
 * them, even though they usually won't be needed. If you have such arguments, use the conventional
 * if/throw idiom instead.
 *
 * <p>Depending on your message arguments, memory may be allocated for boxing and varargs array
 * creation. However, the methods of this class have a large number of overloads that prevent such
 * allocations in many common cases.
 *
 * <p>The message string is not formatted unless the exception will be thrown, so the cost of the
 * string formatting itself should not be a concern.
 *
 * <p>As with any performance concerns, you should consider profiling your code (in a production
 * environment if possible) before spending a lot of effort on tweaking a particular element.
 *
 * <h3>Other types of preconditions</h3>
 *
 * <p>Not every type of precondition failure is supported by these methods. Continue to throw
 * standard JDK exceptions such as {@link java.util.NoSuchElementException} or {@link
 * UnsupportedOperationException} in the situations they are intended for.
 *
 * <h3>Non-preconditions</h3>
 *
 * <p>It is of course possible to use the methods of this class to check for invalid conditions
 * which are <i>not the caller's fault</i>. Doing so is <b>not recommended</b> because it is
 * misleading to future readers of the code and of stack traces. See <a
 * href="https://github.com/google/guava/wiki/ConditionalFailuresExplained">Conditional failures
 * similar to those in this class for non-precondition checks.
 *
 * <h3>{@code java.util.Objects.requireNonNull()}</h3>
 *
 * <p>Projects which use {@code com.google.common} should generally avoid the use of {@link
 * java.util.Objects#requireNonNull(Object)}. Instead, use whichever of {@link
 *
 * <h3>Only {@code %s} is supported</h3>
 *
 * <p>{@code Preconditions} uses {@link Strings#lenientFormat} to format error message template
 * strings. This only supports the {@code "%s"} specifier, not the full range of {@link
 * java.util.Formatter} specifiers. However, note that if the number of arguments does not match the
 * number of occurrences of {@code "%s"} in the format string, {@code Preconditions} will still
 * behave as expected, and will still include all argument values in the error message; the message
 * will simply not be formatted exactly as intended.
 *
 * <h3>More information</h3>
 *
 * <p>See the Guava User Guide on <a
 * href="https://github.com/google/guava/wiki/PreconditionsExplained">using {@code
 * Preconditions}</a>.
 *
 * @author Kevin Bourrillion
 * @since 2.0
 */
@GwtCompatible
public final class Preconditions {
    private Preconditions() {}


    /**
     * Returns the given {@code template} string with each occurrence of {@code "%s"} replaced with
     * the corresponding argument value from {@code args}; or, if the placeholder and argument counts
     * do not match, returns a best-effort form of that string. Will not throw an exception under
     * normal conditions.
     *
     * <p><b>Note:</b> For most string-formatting needs, use {@link String#format String.format},
     * {@link java.io.PrintWriter#format PrintWriter.format}, and related methods. These support the
     * full range of <a
     * href="https://docs.oracle.com/javase/9/docs/api/java/util/Formatter.html#syntax">format
     * specifiers</a>, and alert you to usage errors by throwing {@link
     * java.util.IllegalFormatException}.
     *
     * <p>In certain cases, such as outputting debugging information or constructing a message to be
     * used for another unchecked exception, an exception during string formatting would serve little
     * purpose except to supplant the real information you were trying to provide. These are the cases
     * this method is made for; it instead generates a best-effort string with all supplied argument
     * values present. This method is also useful in environments such as GWT where {@code
     * String.format} is not available. As an example, method implementations of the {@link
     * com.google.common.base.Preconditions} class use this formatter, for both of the reasons just discussed.
     *
     * <p><b>Warning:</b> Only the exact two-character placeholder sequence {@code "%s"} is
     * recognized.
     *
     * @param template a string containing zero or more {@code "%s"} placeholder sequences. {@code
     *     null} is treated as the four-character string {@code "null"}.
     * @param args the arguments to be substituted into the message template. The first argument
     *     specified is substituted for the first occurrence of {@code "%s"} in the template, and so
     *     forth. A {@code null} argument is converted to the four-character string {@code "null"};
     *     non-null values are converted to strings using {@link Object#toString()}.
     * @since 25.1
     */
    // TODO(diamondm) consider using Arrays.toString() for array parameters
    public static String lenientFormat(
        @Nullable String template, @Nullable Object @Nullable ... args) {
        template = String.valueOf(template); // null -> "null"

        if (args == null) {
            args = new Object[] {"(Object[])null"};
        } else {
            for (int i = 0; i < args.length; i++) {
                args[i] = lenientToString(args[i]);
            }
        }

        // start substituting the arguments into the '%s' placeholders
        StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
        int templateStart = 0;
        int i = 0;
        while (i < args.length) {
            int placeholderStart = template.indexOf("%s", templateStart);
            if (placeholderStart == -1) {
                break;
            }
            builder.append(template, templateStart, placeholderStart);
            builder.append(args[i++]);
            templateStart = placeholderStart + 2;
        }
        builder.append(template, templateStart, template.length());

        // if we run out of placeholders, append the extra args in square braces
        if (i < args.length) {
            builder.append(" [");
            builder.append(args[i++]);
            while (i < args.length) {
                builder.append(", ");
                builder.append(args[i++]);
            }
            builder.append(']');
        }

        return builder.toString();
    }

    private static String lenientToString(@Nullable Object o) {
        if (o == null) {
            return "null";
        }
        try {
            return o.toString();
        } catch (Exception e) {
            // Default toString() behavior - see Object.toString()
            String objectToString =
                o.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(o));
            // Logger is created inline with fixed name to avoid forcing Proguard to create another class.
            return "<" + objectToString + " threw " + e.getClass().getName() + ">";
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * @param expression a boolean expression
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * @param expression a boolean expression
     * @param errorMessage the exception message to use if the check fails; will be converted to a
     *     string using {@link String#valueOf(Object)}
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * @param expression a boolean expression
     * @param errorMessageTemplate a template for the exception message should the check fail. The
     *     message is formed by replacing each {@code %s} placeholder in the template with an
     *     argument. These are matched by position - the first {@code %s} gets {@code
     *     errorMessageArgs[0]}, etc. Unmatched arguments will be appended to the formatted message in
     *     square braces. Unmatched placeholders will be left as-is.
     * @param errorMessageArgs the arguments to be substituted into the message template. Arguments
     *     are converted to strings using {@link String#valueOf(Object)}.
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(
        boolean expression,
        @Nullable String errorMessageTemplate,
        @Nullable Object @Nullable ... errorMessageArgs) {
        if (!expression) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, errorMessageArgs));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(boolean b, @Nullable String errorMessageTemplate, char p1) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(boolean b, @Nullable String errorMessageTemplate, int p1) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(boolean b, @Nullable String errorMessageTemplate, long p1) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, char p1, char p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, char p1, int p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, char p1, long p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, char p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, int p1, char p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, int p1, int p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, int p1, long p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, int p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, long p1, char p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, long p1, int p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, long p1, long p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, long p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, char p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, int p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, long p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b,
        @Nullable String errorMessageTemplate,
        @Nullable Object p1,
        @Nullable Object p2,
        @Nullable Object p3) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2, p3));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the calling method.
     *
     * <p>See {@link #checkArgument(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkArgument(
        boolean b,
        @Nullable String errorMessageTemplate,
        @Nullable Object p1,
        @Nullable Object p2,
        @Nullable Object p3,
        @Nullable Object p4) {
        if (!b) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2, p3, p4));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * @param expression a boolean expression
     * @throws IllegalStateException if {@code expression} is false
     */
    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * @param expression a boolean expression
     * @param errorMessage the exception message to use if the check fails; will be converted to a
     *     string using {@link String#valueOf(Object)}
     * @throws IllegalStateException if {@code expression} is false
     */
    public static void checkState(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * @param expression a boolean expression
     * @param errorMessageTemplate a template for the exception message should the check fail. The
     *     message is formed by replacing each {@code %s} placeholder in the template with an
     *     argument. These are matched by position - the first {@code %s} gets {@code
     *     errorMessageArgs[0]}, etc. Unmatched arguments will be appended to the formatted message in
     *     square braces. Unmatched placeholders will be left as-is.
     * @param errorMessageArgs the arguments to be substituted into the message template. Arguments
     *     are converted to strings using {@link String#valueOf(Object)}.
     * @throws IllegalStateException if {@code expression} is false
     */
    public static void checkState(
        boolean expression,
        @Nullable String errorMessageTemplate,
        @Nullable Object @Nullable ... errorMessageArgs) {
        if (!expression) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, errorMessageArgs));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, char p1) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, int p1) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, long p1) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, char p1, char p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, char p1, int p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, char p1, long p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, char p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, int p1, char p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, int p1, int p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, int p1, long p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, int p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, long p1, char p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(boolean b, @Nullable String errorMessageTemplate, long p1, int p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, long p1, long p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, long p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, char p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, int p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, long p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b, @Nullable String errorMessageTemplate, @Nullable Object p1, @Nullable Object p2) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b,
        @Nullable String errorMessageTemplate,
        @Nullable Object p1,
        @Nullable Object p2,
        @Nullable Object p3) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2, p3));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance, but not
     * involving any parameters to the calling method.
     *
     * <p>See {@link #checkState(boolean, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    public static void checkState(
        boolean b,
        @Nullable String errorMessageTemplate,
        @Nullable Object p1,
        @Nullable Object p2,
        @Nullable Object p3,
        @Nullable Object p4) {
        if (!b) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, p1, p2, p3, p4));
        }
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param reference an object reference
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param reference an object reference
     * @param errorMessage the exception message to use if the check fails; will be converted to a
     *     string using {@link String#valueOf(Object)}
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T reference, @Nullable Object errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param reference an object reference
     * @param errorMessageTemplate a template for the exception message should the check fail. The
     *     message is formed by replacing each {@code %s} placeholder in the template with an
     *     argument. These are matched by position - the first {@code %s} gets {@code
     *     errorMessageArgs[0]}, etc. Unmatched arguments will be appended to the formatted message in
     *     square braces. Unmatched placeholders will be left as-is.
     * @param errorMessageArgs the arguments to be substituted into the message template. Arguments
     *     are converted to strings using {@link String#valueOf(Object)}.
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T reference,
        @Nullable String errorMessageTemplate,
        @Nullable Object @Nullable ... errorMessageArgs) {
        if (reference == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, errorMessageArgs));
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, char p1) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, int p1) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, long p1) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, @Nullable Object p1) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, char p1, char p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, char p1, int p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, char p1, long p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, char p1, @Nullable Object p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, int p1, char p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, int p1, int p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, int p1, long p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, int p1, @Nullable Object p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, long p1, char p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, long p1, int p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, long p1, long p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, long p1, @Nullable Object p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, char p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, int p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, long p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, @Nullable Object p2) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj,
        @Nullable String errorMessageTemplate,
        @Nullable Object p1,
        @Nullable Object p2,
        @Nullable Object p3) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2, p3));
        }
        return obj;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * <p>See {@link #checkNotNull(Object, String, Object...)} for details.
     *
     * @since 20.0 (varargs overload since 2.0)
     */
    @CanIgnoreReturnValue
    public static <T extends @NonNull Object> T checkNotNull(
        T obj,
        @Nullable String errorMessageTemplate,
        @Nullable Object p1,
        @Nullable Object p2,
        @Nullable Object p3,
        @Nullable Object p4) {
        if (obj == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, p1, p2, p3, p4));
        }
        return obj;
    }

    /*
     * All recent hotspots (as of 2009) *really* like to have the natural code
     *
     * if (guardExpression) {
     *    throw new BadException(messageExpression);
     * }
     *
     * refactored so that messageExpression is moved to a separate String-returning method.
     *
     * if (guardExpression) {
     *    throw new BadException(badMsg(...));
     * }
     *
     * The alternative natural refactorings into void or Exception-returning methods are much slower.
     * This is a big deal - we're talking factors of 2-8 in microbenchmarks, not just 10-20%. (This is
     * a hotspot optimizer bug, which should be fixed, but that's a separate, big project).
     *
     * The coding pattern above is heavily used in java.util, e.g. in ArrayList. There is a
     * RangeCheckMicroBenchmark in the JDK that was used to test this.
     *
     * But the methods in this class want to throw different exceptions, depending on the args, so it
     * appears that this pattern is not directly applicable. But we can use the ridiculous, devious
     * trick of throwing an exception in the middle of the construction of another exception. Hotspot
     * is fine with that.
     */

    /**
     * Ensures that {@code index} specifies a valid <i>element</i> in an array, list or string of size
     * {@code size}. An element index may range from zero, inclusive, to {@code size}, exclusive.
     *
     * @param index a user-supplied index identifying an element of an array, list or string
     * @param size the size of that array, list or string
     * @return the value of {@code index}
     * @throws IndexOutOfBoundsException if {@code index} is negative or is not less than {@code size}
     * @throws IllegalArgumentException if {@code size} is negative
     */
    @CanIgnoreReturnValue
    public static int checkElementIndex(int index, int size) {
        return checkElementIndex(index, size, "index");
    }

    /**
     * Ensures that {@code index} specifies a valid <i>element</i> in an array, list or string of size
     * {@code size}. An element index may range from zero, inclusive, to {@code size}, exclusive.
     *
     * @param index a user-supplied index identifying an element of an array, list or string
     * @param size the size of that array, list or string
     * @param desc the text to use to describe this index in an error message
     * @return the value of {@code index}
     * @throws IndexOutOfBoundsException if {@code index} is negative or is not less than {@code size}
     * @throws IllegalArgumentException if {@code size} is negative
     */
    @CanIgnoreReturnValue
    public static int checkElementIndex(int index, int size, @Nullable String desc) {
        // Carefully optimized for execution by hotspot (explanatory comment above)
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(badElementIndex(index, size, desc));
        }
        return index;
    }

    private static String badElementIndex(int index, int size, @Nullable String desc) {
        if (index < 0) {
            return lenientFormat("%s (%s) must not be negative", desc, index);
        } else if (size < 0) {
            throw new IllegalArgumentException("negative size: " + size);
        } else { // index >= size
            return lenientFormat("%s (%s) must be less than size (%s)", desc, index, size);
        }
    }

    /**
     * Ensures that {@code index} specifies a valid <i>position</i> in an array, list or string of
     * size {@code size}. A position index may range from zero to {@code size}, inclusive.
     *
     * @param index a user-supplied index identifying a position in an array, list or string
     * @param size the size of that array, list or string
     * @return the value of {@code index}
     * @throws IndexOutOfBoundsException if {@code index} is negative or is greater than {@code size}
     * @throws IllegalArgumentException if {@code size} is negative
     */
    @CanIgnoreReturnValue
    public static int checkPositionIndex(int index, int size) {
        return checkPositionIndex(index, size, "index");
    }

    /**
     * Ensures that {@code index} specifies a valid <i>position</i> in an array, list or string of
     * size {@code size}. A position index may range from zero to {@code size}, inclusive.
     *
     * @param index a user-supplied index identifying a position in an array, list or string
     * @param size the size of that array, list or string
     * @param desc the text to use to describe this index in an error message
     * @return the value of {@code index}
     * @throws IndexOutOfBoundsException if {@code index} is negative or is greater than {@code size}
     * @throws IllegalArgumentException if {@code size} is negative
     */
    @CanIgnoreReturnValue
    public static int checkPositionIndex(int index, int size, @Nullable String desc) {
        // Carefully optimized for execution by hotspot (explanatory comment above)
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException(badPositionIndex(index, size, desc));
        }
        return index;
    }

    private static String badPositionIndex(int index, int size, @Nullable String desc) {
        if (index < 0) {
            return lenientFormat("%s (%s) must not be negative", desc, index);
        } else if (size < 0) {
            throw new IllegalArgumentException("negative size: " + size);
        } else { // index > size
            return lenientFormat("%s (%s) must not be greater than size (%s)", desc, index, size);
        }
    }

    /**
     * Ensures that {@code start} and {@code end} specify valid <i>positions</i> in an array, list or
     * string of size {@code size}, and are in order. A position index may range from zero to {@code
     * size}, inclusive.
     *
     * @param start a user-supplied index identifying a starting position in an array, list or string
     * @param end a user-supplied index identifying an ending position in an array, list or string
     * @param size the size of that array, list or string
     * @throws IndexOutOfBoundsException if either index is negative or is greater than {@code size},
     *     or if {@code end} is less than {@code start}
     * @throws IllegalArgumentException if {@code size} is negative
     */
    public static void checkPositionIndexes(int start, int end, int size) {
        // Carefully optimized for execution by hotspot (explanatory comment above)
        if (start < 0 || end < start || end > size) {
            throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
        }
    }

    private static String badPositionIndexes(int start, int end, int size) {
        if (start < 0 || start > size) {
            return badPositionIndex(start, size, "start index");
        }
        if (end < 0 || end > size) {
            return badPositionIndex(end, size, "end index");
        }
        // end < start
        return lenientFormat("end index (%s) must not be less than start index (%s)", end, start);
    }
}
