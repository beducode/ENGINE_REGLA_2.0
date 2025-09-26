CREATE OR REPLACE FUNCTION REVERSE_OPERATORS(input_text TEXT)
RETURNS TEXT AS $$
DECLARE
    i INT := 1;
    len INT := length(input_text);
    in_quotes BOOLEAN := FALSE;
    ch TEXT;
    next_ch TEXT;
    lookahead TEXT;
    result TEXT := '';
BEGIN
    WHILE i <= len LOOP
        ch := substr(input_text, i, 1);

        -- Toggle in_quotes flag when encountering a single quote
        IF ch = '''' THEN
            in_quotes := NOT in_quotes;
            result := result || ch;
            i := i + 1;
            CONTINUE;
        END IF;

        -- Only replace operators when NOT inside quotes
        IF NOT in_quotes THEN
            next_ch := substr(input_text, i+1, 1);
            lookahead := substr(input_text, i, 8); -- enough to catch "NOT IN", "NOT LIKE"

            -- === Multi-char operators ===
            IF substr(lookahead,1,6) ILIKE 'NOT IN' 
               AND (i = 1 OR substr(input_text,i-1,1) ~ '\s') THEN
                result := result || 'IN';
                i := i + 6;
                CONTINUE;
            ELSIF substr(lookahead,1,2) ILIKE 'IN' 
               AND (i = 1 OR substr(input_text,i-1,1) ~ '\s') THEN
                result := result || 'NOT IN';
                i := i + 2;
                CONTINUE;
            ELSIF substr(lookahead,1,8) ILIKE 'NOT LIKE' 
               AND (i = 1 OR substr(input_text,i-1,1) ~ '\s') THEN
                result := result || 'LIKE';
                i := i + 8;
                CONTINUE;
            ELSIF substr(lookahead,1,4) ILIKE 'LIKE' 
               AND (i = 1 OR substr(input_text,i-1,1) ~ '\s') THEN
                result := result || 'NOT LIKE';
                i := i + 4;
                CONTINUE;
            END IF;

            -- === Symbol operators ===
            -- >=
            IF ch = '>' AND next_ch = '=' THEN
                result := result || '<=';
                i := i + 2;
                CONTINUE;
            END IF;

            -- <=
            IF ch = '<' AND next_ch = '=' THEN
                result := result || '>=';
                i := i + 2;
                CONTINUE;
            END IF;

            -- <>
            IF ch = '<' AND next_ch = '>' THEN
                result := result || '=';
                i := i + 2;
                CONTINUE;
            END IF;

            -- >
            IF ch = '>' THEN
                result := result || '<';
                i := i + 1;
                CONTINUE;
            END IF;

            -- <
            IF ch = '<' THEN
                result := result || '>';
                i := i + 1;
                CONTINUE;
            END IF;

            -- =
            IF ch = '=' THEN
                result := result || '<>';
                i := i + 1;
                CONTINUE;
            END IF;
        END IF;

        -- Default: just append the character
        result := result || ch;
        i := i + 1;
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
