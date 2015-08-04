package org.apache.nifi.processors.amqp.util;

import static org.apache.nifi.processors.amqp.util.AmqpProperties.JKS_FILE_PREFIX;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

public class KeyStoreToJKS {

	public static String convertToJKS(final String aKeystore, final String aKeystorePasswd,
			final Path dir, final String id) throws GeneralSecurityException, IOException {
		File keystoreFile = new File(aKeystore);
		String filename = keystoreFile.getName();
		String[] tokens = filename.split("\\.");
		filename = tokens[0];
		
		Path createdJKSFile = dir.resolve(JKS_FILE_PREFIX + id + "-" + filename + ".jks");
		if(!Files.exists(createdJKSFile)){
			final KeyStore ks = KeyStore.getInstance("PKCS12");
			try(final FileInputStream fis = new FileInputStream(aKeystore); final OutputStream fos = Files.newOutputStream(createdJKSFile)){
				ks.load(fis, aKeystorePasswd.toCharArray());
				final KeyStore ksCopy = KeyStore.getInstance("JKS");
				ksCopy.load(null, null);
				final Enumeration<String> aliases = ks.aliases();
				while (aliases.hasMoreElements()){
					String alias = aliases.nextElement();
					if (ks.isKeyEntry(alias)){
						final Key key = ks.getKey(alias, aKeystorePasswd.toCharArray());
						final Certificate[] certChain = ks.getCertificateChain(alias);
						ksCopy.setKeyEntry(alias, key, aKeystorePasswd.toCharArray(), certChain);
					}
				}
				ksCopy.store(fos, aKeystorePasswd.toCharArray());
				fos.flush();
			}
		}
		return createdJKSFile.toAbsolutePath().toString();
		
	}

}
