package adrien.util;

import java.io.File;
import java.io.FileFilter;

public class FileFilters {

	public static FileFilter havingExtension(final String expectedExtension) {
		return new FileFilter() {
			
			@Override
			public boolean accept(File file) {
				String fileName = file.getName();
				String extension = fileName.substring(fileName.indexOf(".") + 1);
				return extension.equalsIgnoreCase(expectedExtension);
			}
			
		};
	}
	
}
