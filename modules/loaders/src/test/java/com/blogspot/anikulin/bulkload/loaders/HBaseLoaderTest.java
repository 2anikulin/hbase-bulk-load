package com.blogspot.anikulin.bulkload.loaders;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public class HBaseLoaderTest {

    @Test
    public void mainTest() throws IOException {
        HBaseLoader loader = mock(HBaseLoader.class);

        when(
                loader.createClient(Mockito.anyString(), Mockito.anyString())
        ).thenReturn(
                null
        );

        loader.main(new String[]{"0", "1000000"});
    }
}
