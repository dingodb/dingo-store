package io.dingodb.sdk.common.serial;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class MyDecimal {

    /**
     * max count of words in buf.
     */
    private static final int MaxWordBufLen = 9;

    /**
     * max value of a word.
     */
    private static final int wordMax = 1000000000 - 1;

    /**
     * dig mask.
     */
    private static final int digMask = 100000000;

    /**
     * digits per word.
     */
    private static final int digitsPerWord = 9;

    /**
     * word size.
     */
    private static final int wordSize = 4;

    /**
     * word buffer length.
     */
    private static final int wordBufLen = 9;

    /**
     * mysql max decimal scale.
     */
    private static final int MaxDecimalScale = 30;

    /**
     * length of div9 array.
     */
    private static final int div9Len = 128;

    /**
     * decimal error code.
     */
    private static enum DecimalError {
        None,
        ErrOverflow,
        ErrTruncate,
        ErrBadNumber
    }

    /**
     * decimal context.
     */
    @AllArgsConstructor
    @Getter
    @Setter
    private static class DecimalContext {
        private int start;
        private int end;
        private int sum;
        private int newCarry;
        private int wordIdx;
        private int digitsIntLocal;
        private int newWordInt;
        private int newWordFrac;
        private int decimalBinSize;
        private DecimalError err;

        DecimalContext() {
            start = 0;
            end = 0;
            sum = 0;
            newCarry = 0;
            wordIdx = 0;
            digitsIntLocal = 0;
            newWordInt = 0;
            newWordFrac = 0;
            decimalBinSize = 0;
            err = DecimalError.None;
        }
    }

    /**
     * div9 for digits to words.
     */
    private static final int[] div9 = new int[]{
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3, 3,
            4, 4, 4, 4, 4, 4, 4, 4, 4,
            5, 5, 5, 5, 5, 5, 5, 5, 5,
            6, 6, 6, 6, 6, 6, 6, 6, 6,
            7, 7, 7, 7, 7, 7, 7, 7, 7,
            8, 8, 8, 8, 8, 8, 8, 8, 8,
            9, 9, 9, 9, 9, 9, 9, 9, 9,
            10, 10, 10, 10, 10, 10, 10, 10, 10,
            11, 11, 11, 11, 11, 11, 11, 11, 11,
            12, 12, 12, 12, 12, 12, 12, 12, 12,
            13, 13, 13, 13, 13, 13, 13, 13, 13,
            14, 14
    };

    /**
     * digits 2 bytes.
     */
    private static final int[] dig2bytes = new int[]{
            0, 1, 1, 2, 2, 3, 3, 4, 4, 4,
    };

    /**
     * power10.
     */
    private static int powers10[] = new int[] {
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000
    };

    @Getter
    @Setter
    private int precision;

    @Getter
    @Setter
    private int scale;

    /**
     * The number of *decimal* digits before the point.
     */
    @Getter
    @Setter
    private int digitsInt = 0;

    /**
     * the number of decimal digits after the point.
     */
    @Getter
    @Setter
    private int digitsFrac = 0;

    /**
     * result fraction digits.
     */
    @Getter
    @Setter
    private int resultFrac = 0;

    /**
     * decimal的正负号，true为负，false为正。
     */
    @Getter
    @Setter
    private boolean negative = false;

    /**
     * The buffer to store the decimal words.
     */
    @Getter
    private final int wordBuf[] = new int[MaxWordBufLen];

    private int digitsToWords(int digits) {
        if(digits+digitsPerWord-1 >= 0 && digits+digitsPerWord-1 < div9Len) {
            return div9[digits+digitsPerWord-1];
        }
        return (digits + digitsPerWord - 1) / digitsPerWord;
    }

    private int countLeadingZeroes(int i, int word) {
        int leading = 0;
        while(word < powers10[i]) {
            i--;
            leading++;
        }
        return leading;
    }

    private DecimalContext removeLeadingZeros() {
        DecimalContext decimalInProcess = new DecimalContext();
        decimalInProcess.digitsIntLocal = digitsInt;
        int i = ((decimalInProcess.digitsIntLocal - 1) % digitsPerWord) + 1;
        while (decimalInProcess.digitsIntLocal > 0 && wordBuf[decimalInProcess.wordIdx] == 0) {
            decimalInProcess.digitsIntLocal -= i;
            i = digitsPerWord;
            decimalInProcess.wordIdx++;
        }
        if (decimalInProcess.digitsIntLocal > 0) {
            decimalInProcess.digitsIntLocal -= countLeadingZeroes((decimalInProcess.digitsIntLocal-1)%digitsPerWord, wordBuf[decimalInProcess.wordIdx]);
        } else {
            decimalInProcess.digitsIntLocal = 0;
        }
        return decimalInProcess;
    }

    private DecimalContext DecimalBinSize(int precision, int frac) {
        DecimalContext decimalContext = new DecimalContext();
        int digitsInt = precision - frac;
        int wordsInt = digitsInt / digitsPerWord;
        int wordsFrac = frac / digitsPerWord;
        int xInt = digitsInt - wordsInt*digitsPerWord;
        int xFrac = frac - wordsFrac*digitsPerWord;
        if (xInt < 0 || xInt >= dig2bytes.length || xFrac < 0 || xFrac >= dig2bytes.length) {
            decimalContext.decimalBinSize = 0;
            decimalContext.err = DecimalError.ErrBadNumber;
            return decimalContext;
        }

        decimalContext.decimalBinSize = wordsInt*wordSize + dig2bytes[xInt] + wordsFrac*wordSize + dig2bytes[xFrac];
        decimalContext.err = DecimalError.None;
        return decimalContext;
    }

    private int readWord(byte[] b, int index, int size) {
        int x = 0;
        switch (size) {
            case 1: {
                x = b[index];
                break;
            }
            case 2: {
                int a1 = b[index] << 8;
                int a2 = b[index + 1] & 0xFF;
                x = a1 + a2;

                //x = (b[index] << 8) & 0xFFFF + b[index + 1] & 0xFF;
                break;
            }
            case 3: {
                if ((b[index] & 128) > 0) {
                    x = (255) << 24 | ((b[index]) << 16) & 0xFFFFFF | ((b[index + 1]) << 8) & 0xFFFF | (b[index + 2] & 0xFF);
                } else {
                    x = ((b[index]) << 16) & 0xFFFFFF | ((b[index + 1]) << 8) & 0xFFFF | (b[index + 2] & 0xFF);
                }
                break;
            }
            case 4: {
                x = (b[index + 3] & 0xFF) +
                        ((b[index + 2] << 8) & 0xFFFF) +
                        ((b[index + 1] << 16) & 0xFFFFFF) +
                        ((b[index] << 24) & 0xFFFFFFFF);
                break;
            }
        }
        return x;
    }

    private void writeWord(byte[] b, int start,  int word, int size) {
        int v = word;
        switch(size) {
            case 1: {
                b[start] = (byte) word;
                break;
            }
            case 2: {
                b[start] = (byte) (v >> 8);
                b[start + 1] = (byte) (v);
                break;
            }
            case 3: {
                b[start] = (byte) (v >> 16);
                b[start + 1] = (byte) (v >> 8);
                b[start + 2] = (byte) (v);
                break;
            }
            case 4: {
                b[start] = (byte) (v >> 24);
                b[start + 1] = (byte) (v >> 16);
                b[start + 2] = (byte) (v >> 8);
                b[start + 3] = (byte) (v);
                break;
            }
        }
    }

    private DecimalContext fixWordCntError(int wordsInt, int wordsFrac) {
        DecimalContext decimalContext = new DecimalContext();
        if(wordsInt + wordsFrac > wordBufLen) {
            if(wordsInt > wordBufLen) {
                decimalContext.setNewWordInt(wordBufLen);
                decimalContext.setNewWordFrac(0);
                decimalContext.setErr(DecimalError.ErrOverflow);

                return decimalContext;
            }

            decimalContext.setNewWordInt(wordsInt);
            decimalContext.setNewWordFrac(wordBufLen - wordsInt);
            decimalContext.setErr(DecimalError.ErrTruncate);

            return decimalContext;
        }

        decimalContext.setNewWordInt(wordsInt);
        decimalContext.setNewWordFrac(wordsFrac);
        decimalContext.setErr(DecimalError.None);

        return decimalContext;
    }

    private String dealWithE(String string) {
        int ePos = string.toLowerCase().indexOf("e");
        int eVal = Integer.parseInt(string.substring(ePos+1, string.length()));

        int negative = -1;  //no flag, default +.
        if( string.charAt(0) == '-' ) {
            negative = 1;   // flag for '-'.
        } else if( string.charAt(0) == '+' ) {
            negative = 0;   // flag for '+'.
        }

        int valueStart = (negative == -1) ? 0 : 1;
        boolean hasPoint = string.contains(".");
        if(hasPoint) {
            int pointPos = string.indexOf(".");
            String intPartString = string.substring(valueStart, pointPos);
            String fracPartString = string.substring(pointPos + 1, ePos);

            StringBuilder builder = new StringBuilder();
            if (negative == 1) {
                builder.append("-");
            } else if(negative == 0) {
                builder.append("+");
            }

            int newPointPos = pointPos + eVal;
            if(newPointPos < 0 ) {
                int appendingZeroCount = Math.abs(newPointPos);
                if(negative != -1 && newPointPos < 0) {
                    appendingZeroCount++;
                }
                return builder.append("0.")
                        .append(String.valueOf('0').repeat(appendingZeroCount))
                        .append(intPartString)
                        .append(fracPartString).toString();
            } else if(newPointPos > intPartString.length() + fracPartString.length()) {
                int appendingZeroCount = newPointPos - intPartString.length() - fracPartString.length();
                if(negative != -1) {
                    appendingZeroCount--;
                }
                return builder.append(intPartString)
                        .append(fracPartString)
                        .append(String.valueOf('0').repeat(appendingZeroCount)).toString();
            } else {
                return builder.append(intPartString)
                        .append(fracPartString)
                        .insert(newPointPos, '.').toString();
            }
        } else {
            StringBuilder builder = new StringBuilder();
            if (negative == 1) {
                builder.append("-");
            } else if(negative == 0) {
                builder.append("+");
            }

            String finalString;
            int newPointPos = eVal;
            if(newPointPos < 0 ) {
                int appendingZeroCount = Math.abs(newPointPos);
                return builder.append(String.valueOf('0').repeat(Math.abs(newPointPos)))
                        .append(string).toString();
            } else if(newPointPos > string.length()) {
                int appendingZeroCount = newPointPos - string.length();
                return builder.append(string).append(String.valueOf('0').repeat(Math.abs(newPointPos))).toString();
            } else {
                int finalPointPos = ((negative == -1) ? 0 : 1) + newPointPos;
                return builder.append(string).insert(finalPointPos, '.').toString();
            }
        }
    }

    public MyDecimal(String string, int prec, int scal) {
        string = string.trim();
        precision = prec;
        scale = scal;

        if(string.isEmpty()) {
            throw new IllegalArgumentException("Decimal type: Empty string is not allowed.");
        }

        if(string.contains("e") || string.contains("E")) {
            string = dealWithE(string);
        }

        int curStringPos = 0;
        char flag = string.charAt(curStringPos);
        switch (flag) {
            case '-':
            {
                negative = true;
                //fallthrough
            }
            case '+':
            {
                curStringPos++;
            }
        }

        int strIdx = curStringPos;
        while (strIdx < string.length() && Character.isDigit(string.charAt(strIdx))) {
            strIdx++;
        }
        int digitsIntLocal = strIdx - curStringPos;
        int digitsFracLocal = 0;
        int endIdx = 0;

        if(strIdx < string.length() && string.charAt(strIdx) == '.') {
            endIdx = strIdx + 1;
            while(endIdx < string.length() && Character.isDigit(string.charAt(endIdx))) {
                endIdx++;
            }

            digitsFracLocal = endIdx - strIdx - 1;
        } else {
            endIdx = strIdx;
        }

        if(digitsIntLocal + digitsFracLocal == 0) {
            throw new IllegalArgumentException("Decimal type: No digits in string.");
        }

        //To be compatible with MySQL rounding for decimal.
        if(digitsFracLocal != 0 && scal < digitsFracLocal) {
            int lastDigitPos = 1 - ((negative) ? 1 : 0);
            int lastDigit = Integer.parseInt(String.valueOf(string.charAt(digitsIntLocal + digitsFracLocal - lastDigitPos)));
            lastDigit = lastDigit < 5 ? lastDigit : lastDigit + 1;
            string = string.substring(0, digitsIntLocal + digitsFracLocal - lastDigitPos) + lastDigit;
            digitsFracLocal = scal;
        }

        int wordsInt = digitsToWords(digitsIntLocal);
        int wordsFrac = digitsToWords(digitsFracLocal);

        DecimalContext decimalContext = fixWordCntError(wordsInt, wordsFrac);
        wordsInt = decimalContext.getNewWordInt();
        wordsFrac = decimalContext.getNewWordFrac();
        if(decimalContext.getErr() != DecimalError.None) {
            digitsFracLocal = wordsFrac * digitsPerWord;
            if(decimalContext.getErr() == DecimalError.ErrOverflow) {
                digitsIntLocal = wordsInt * digitsPerWord;
            }
        }

        digitsInt = digitsIntLocal;
        digitsFrac = digitsFracLocal;

        int wordIdx = wordsInt;
        int strIdxTmp = strIdx;
        int word = 0;
        int innerIdx = 0;
        while(digitsIntLocal > 0) {
            digitsIntLocal--;
            strIdx--;
            word += (int)(string.charAt(strIdx) - '0') * powers10[innerIdx];
            innerIdx++;
            if(innerIdx == digitsPerWord) {
                wordIdx--;
                wordBuf[wordIdx] = word;
                word = 0;
                innerIdx = 0;
            }
        }
        if(innerIdx != 0) {
            wordIdx--;
            wordBuf[wordIdx] = word;
        }

        wordIdx = wordsInt;
        strIdx = strIdxTmp;
        word = 0;
        innerIdx = 0;
        while (digitsFracLocal > 0) {
            digitsFracLocal--;
            strIdx++;
            word = (int)(string.charAt(strIdx) - '0') + word * 10;
            innerIdx++;
            if(innerIdx == digitsPerWord) {
                wordBuf[wordIdx] = word;
                wordIdx++;
                word = 0;
                innerIdx = 0;
            }
        }
        if(innerIdx != 0) {
            wordBuf[wordIdx] = word * powers10[digitsPerWord - innerIdx];
        }

        if(endIdx+1 <= string.length()) {
            throw new IllegalArgumentException("Decimal type: String contained invalid characters or E at the tail.");
        }

        boolean allZero = true;
        for (int i = 0; i < wordBufLen; i++) {
            if (wordBuf[i] != 0) {
                allZero = false;
                break;
            }
        }
        if(allZero) {
            negative = false;
        }
        resultFrac = digitsFrac;
        return;
    }

    private byte[] writeBin(int precision, int frac) {
        if(precision > digitsPerWord * MyDecimal.MaxWordBufLen || precision < 0 || frac > MyDecimal.MaxDecimalScale || frac < 0) {
            throw new IllegalArgumentException("Decimal type: Precision or frac out of range.");
        }

        int mask = 0;
        if(negative) {
            mask = -1;
        }

        int digitsIntLocal = precision - frac;
        int wordsInt = digitsIntLocal / digitsPerWord;
        int leadingDigits = digitsIntLocal - wordsInt*digitsPerWord;
        int wordsFracLocal = frac / digitsPerWord;
        int trailingDigits = frac - wordsFracLocal*digitsPerWord;

        int wordsFracFrom = digitsFrac / digitsPerWord;
        int trailingDigitsFrom = digitsFrac - wordsFracFrom*digitsPerWord;
        int intSize = wordsInt*wordSize + dig2bytes[leadingDigits];
        int fracSize = wordsFracLocal*wordSize + dig2bytes[trailingDigits];
        int fracSizeFrom = wordsFracFrom*wordSize + dig2bytes[trailingDigitsFrom];
        int originIntSize = intSize;
        int originFracSize = fracSize;

        byte[] buf = new byte[intSize+fracSize];
        int binIdx = 0;

        DecimalContext decimalContext = removeLeadingZeros();
        int wordIdxFrom = decimalContext.wordIdx;
        int digitsIntFrom = decimalContext.digitsIntLocal;

        if(digitsIntFrom+fracSizeFrom == 0) {
            mask = 0;
            digitsIntLocal = 1;
        }

        int wordsIntFrom = digitsIntFrom / digitsPerWord;
        int leadingDigitsFrom = digitsIntFrom - wordsIntFrom*digitsPerWord;
        int iSizeFrom = wordsIntFrom*wordSize + dig2bytes[leadingDigitsFrom];

        if (digitsIntLocal < digitsIntFrom) {
            wordIdxFrom += wordsIntFrom - wordsInt;
            if (leadingDigitsFrom > 0) {
                wordIdxFrom++;
            }
            if (leadingDigits > 0) {
                wordIdxFrom--;
            }
            wordsIntFrom = wordsInt;
            leadingDigitsFrom = leadingDigits;
            throw new RuntimeException("Decimal type: overflow.");
        } else if (intSize > iSizeFrom) {
            while (intSize > iSizeFrom) {
                intSize--;
                buf[binIdx] = (byte)mask;
                binIdx++;
            }
        }

        if ((fracSize < fracSizeFrom) ||
                (fracSize == fracSizeFrom && (trailingDigits <= trailingDigitsFrom || wordsFracLocal <= wordsFracFrom))) {
            if (fracSize < fracSizeFrom || (fracSize == fracSizeFrom && trailingDigits < trailingDigitsFrom) || (fracSize == fracSizeFrom && wordsFracLocal < wordsFracFrom)) {
                throw new RuntimeException("Decimal type: truncated.");
            }
            wordsFracFrom = wordsFracLocal;
            trailingDigitsFrom = trailingDigits;
        } else if (fracSize > fracSizeFrom && trailingDigitsFrom > 0) {
            if (wordsFracLocal == wordsFracFrom) {
                trailingDigitsFrom = trailingDigits;
                fracSize = fracSizeFrom;
            } else {
                wordsFracFrom++;
                trailingDigitsFrom = 0;
            }
        }
        // xIntFrom part
        if (leadingDigitsFrom > 0) {
            int i = dig2bytes[leadingDigitsFrom];
            int x = (wordBuf[wordIdxFrom] % powers10[leadingDigitsFrom]) ^ mask;
            wordIdxFrom++;
            writeWord(buf, binIdx, x, i);
            binIdx += i;
        }

        // wordsInt + wordsFrac part.
        for (int stop = wordIdxFrom + wordsIntFrom + wordsFracFrom; wordIdxFrom < stop; binIdx += wordSize) {
            int x = wordBuf[wordIdxFrom] ^ mask;
            wordIdxFrom++;
            writeWord(buf, binIdx, x, 4);
        }

        // xFracFrom part
        if (trailingDigitsFrom > 0) {
            int x = 0;
            int i = dig2bytes[trailingDigitsFrom];
            int lim = trailingDigits;
            if (wordsFracFrom < wordsFracLocal) {
                lim = digitsPerWord;
            }

            while (trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i) {
                trailingDigitsFrom++;
            }
            x = (wordBuf[wordIdxFrom] / powers10[digitsPerWord-trailingDigitsFrom]) ^ mask;
            writeWord(buf,binIdx, x, i);
            binIdx += i;
        }
        if (fracSize > fracSizeFrom) {
            int binIdxEnd = originIntSize + originFracSize;
            while (fracSize > fracSizeFrom && binIdx < binIdxEnd) {
                fracSize--;
                buf[binIdx] = (byte)(mask);
                binIdx++;
            }
        }
        buf[0] ^= 0x80;
        return buf;
    }

    public byte[] toBin() {
        return toBin(precision, scale);
    }

    public byte[] toBin(int precision, int frac) {
        return writeBin(precision, frac);
    }

    public void FromBin(byte[] bin, int precision, int frac) {
        int binSize = 0;
        if (bin.length == 0) {
            throw new RuntimeException("Decimal type: bin length is zero");
        }
        int digitsIntLocal = precision - frac;
        int wordsInt = digitsIntLocal / digitsPerWord;
        int leadingDigits = digitsIntLocal - wordsInt*digitsPerWord;
        int wordsFrac = frac / digitsPerWord;
        int trailingDigits = frac - wordsFrac*digitsPerWord;
        int wordsIntTo = wordsInt;
        if (leadingDigits > 0) {
            wordsIntTo++;
        }
        int wordsFracTo = wordsFrac;
        if (trailingDigits > 0) {
            wordsFracTo++;
        }

        int binIdx = 0;
        int mask = (int)(-1);
        if ((bin[binIdx]&0x80) > 0) {
            mask = 0;
        }

        DecimalContext decimalContext = DecimalBinSize(precision, frac);
        binSize = decimalContext.decimalBinSize;
        DecimalError err = decimalContext.getErr();

        if(err != DecimalError.None) {
            throw new RuntimeException("Decimal type: Bad number.");
        }
        if (binSize < 0 || binSize > 40) {
            throw new RuntimeException("Decimal type: Bad number.");
        }

        bin[0] ^= 0x80;

        int oldWordsIntTo = wordsIntTo;
        DecimalContext decimalInProcess = fixWordCntError(wordsIntTo, wordsFracTo);
        wordsIntTo = decimalInProcess.newWordInt;
        wordsFracTo = decimalInProcess.newWordFrac;
        DecimalError err2 = decimalContext.getErr();

        if(err != DecimalError.None) {
            if(wordsIntTo < oldWordsIntTo) {
                binIdx += dig2bytes[leadingDigits] + (wordsInt-wordsIntTo)*wordSize;
            } else {
                trailingDigits = 0;
                wordsFrac = wordsFracTo;
            }
        }

        negative = (mask != 0);
        digitsInt = wordsInt*digitsPerWord + leadingDigits;
        digitsFrac = wordsFrac*digitsPerWord + trailingDigits;

        int wordIdx = 0;
        if (leadingDigits > 0) {
            int i = dig2bytes[leadingDigits];
            int x = readWord(bin,binIdx, i);
            binIdx += i;
            wordBuf[wordIdx] = x ^ mask;
            if ((long)(wordBuf[wordIdx]) >= (long)(powers10[leadingDigits+1])) {
                throw new RuntimeException("Decimal type: Bad number.");
            }
            if (wordIdx > 0 || wordBuf[wordIdx] != 0) {
                wordIdx++;
            } else {
                digitsInt -= (byte)(leadingDigits);
            }
        }
        for (int stop = binIdx + wordsInt*wordSize; binIdx < stop; binIdx += wordSize) {
            wordBuf[wordIdx] = readWord(bin,binIdx, 4) ^ mask;
            if ((int)(wordBuf[wordIdx]) > wordMax) {
                throw new RuntimeException("Decimal type: bad number.");
            }
            if (wordIdx > 0 || wordBuf[wordIdx] != 0) {
                wordIdx++;
            } else {
                digitsInt -= digitsPerWord;
            }
        }

        for (int stop = binIdx + wordsFrac*wordSize; binIdx < stop; binIdx += wordSize) {
            wordBuf[wordIdx] = readWord(bin,binIdx, 4) ^ mask;
            if ((int)(wordBuf[wordIdx]) > wordMax) {
                throw new RuntimeException("Decimal type: bad number.");
            }
            wordIdx++;
        }

        if (trailingDigits > 0) {
            int i = dig2bytes[trailingDigits];
            int x = readWord(bin,binIdx, i);
            wordBuf[wordIdx] = (x ^ mask) * powers10[digitsPerWord-trailingDigits];
            if ((int)(wordBuf[wordIdx]) > wordMax) {
                throw new RuntimeException("Decimal type: Bad number.");
            }
        }

        if (digitsInt == 0 && digitsFrac == 0) {
            throw new RuntimeException("Decimal type: bad number.");
        }
        resultFrac = frac;
    }

    public MyDecimal(byte[] buf, int prec, int scal) {
        precision = prec;
        scale = scal;
        FromBin(buf, precision, scale);
    }

    private int stringSize() {
        return (int)(digitsInt + digitsFrac + 3);
    }

    public String decimalToString() {
        int digitsFracLocal = digitsFrac;
        DecimalContext decimalInProcess = removeLeadingZeros();
        int wordStartIdx = decimalInProcess.wordIdx;
        int digitsIntLocal = decimalInProcess.digitsIntLocal;


        if(digitsIntLocal+digitsFracLocal == 0) {
            digitsIntLocal = 1;
            wordStartIdx = 0;
        }

        int digitsIntLen = digitsIntLocal;
        if(digitsIntLen == 0) {
            digitsIntLen = 1;
        }
        int digitsFracLen = digitsFracLocal;
        int length = digitsIntLen + digitsFracLen;
        if (negative) {
            length++;
        }
        if (digitsFracLocal > 0) {
            length++;
        }

        byte[] str = new byte[length];
        int strIdx = 0;

        if (negative) {
            str[strIdx] = '-';
            strIdx++;
        }

        int fill = 0;
        if (digitsFracLocal > 0) {
            int fracIdx = strIdx + digitsIntLen;
            fill = digitsFracLen - digitsFracLocal;
            int wordIdx = wordStartIdx + digitsToWords(digitsIntLocal);
            str[fracIdx] = '.';
            fracIdx++;
            for (; digitsFracLocal > 0; digitsFracLocal -= digitsPerWord) {
                int x = wordBuf[wordIdx];
                wordIdx++;
                for (int i = Math.min(digitsFracLocal, digitsPerWord); i > 0; i--) {
                    int y = x / digMask;
                    str[fracIdx] = (byte)((byte)y + '0');
                    fracIdx++;
                    x -= y * digMask;
                    x *= 10;
                }
            }
            for (; fill > 0; fill--) {
                str[fracIdx] = '0';
                fracIdx++;
            }
        }
        fill = digitsIntLen - digitsIntLocal;
        if (digitsIntLocal == 0) {
            fill--; /* symbol 0 before digital point */
        }
        for (; fill > 0; fill--) {
            str[strIdx] = '0';
            strIdx++;
        }

        if (digitsIntLocal > 0) {
            strIdx += digitsIntLocal;
            int wordIdx = wordStartIdx + digitsToWords(digitsIntLocal);
            for (; digitsIntLocal > 0; digitsIntLocal -= digitsPerWord) {
                wordIdx--;
                int x = wordBuf[wordIdx];
                for (int i = Math.min(digitsIntLocal, digitsPerWord); i > 0; i--) {
                    int y = x / 10;
                    strIdx--;
                    str[strIdx] = (byte)('0' + (byte)(x-y*10));
                    x = y;
                }
            }
        } else {
            str[strIdx] = '0';
        }
        return new String(str);
    }

}
