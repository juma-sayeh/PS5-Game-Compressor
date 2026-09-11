(function (global) {
  'use strict';

  const DEFAULT_LOCALE = 'en';
  const catalogs = Object.create(null);
  const localeMeta = Object.create(null);
  const listeners = [];
  let currentLocale = DEFAULT_LOCALE;

  const english = {
    'app.name': 'Game Compressor',
    'app.documentTitle': 'Game Compressor',

    'common.applyVersion': 'Apply Version',
    'common.cancel': 'Cancel',
    'common.cancelled': 'Cancelled',
    'common.cached': 'Cached',
    'common.change': 'Change',
    'common.close': 'Close',
    'common.continueAnyway': 'Continue anyway',
    'common.current': 'Current',
    'common.delete': 'Delete',
    'common.destination': 'Destination',
    'common.done': 'Done',
    'common.download': 'Download',
    'common.failed': 'Failed',
    'common.files': 'Files',
    'common.image': 'Image',
    'common.installed': 'Installed',
    'common.latest': 'Latest',
    'common.output': 'Output',
    'common.select': 'Select',
    'common.selected': 'Selected',
    'common.success': 'Success',
    'common.pending': 'Pending',
    'common.source': 'Source',
    'common.start': 'Start',
    'common.unavailable': 'Unavailable',
    'common.working': 'Working...',

    'accessibility.about': 'About',
    'accessibility.aboutApp': 'About Game Compressor',
    'accessibility.cancelOperation': 'Cancel operation',
    'accessibility.colorMode': 'Color mode',
    'accessibility.copyPath': 'Copy path',
    'accessibility.darkMode': 'Dark mode',
    'accessibility.history': 'History',
    'accessibility.lightMode': 'Light mode',
    'accessibility.love': 'love',
    'accessibility.moreActions': 'More actions',
    'accessibility.palestine': 'Palestine',
    'accessibility.terminateApp': 'Terminate Game Compressor',

    'loading.scanningGames': 'Scanning games',
    'loading.gamesList': 'Looking for installed and compressed game entries.',
    'loading.gamesDetail': 'This can take a moment on PS5 storage.',
    'loading.storageTargets': 'Loading storage targets',
    'loading.externalStorage': 'Loading external storage',
    'games.scanFailed': 'Scan failed',
    'games.noneFound': 'No games found',
    'games.count.one': '{count} game',
    'games.count.other': '{count} games',

    'about.builtBy': 'Built by Juma Sayeh',
    'about.testedBy': 'Tested by Osama Abualia',
    'about.madeWith': 'Made with',
    'about.inPalestine': 'in',

    'theme.saveFailed': 'Could not save display mode',
    'settings.language': 'Language',
    'settings.systemLanguage': 'System language',

    'header.freeSpace': 'Free Space',
    'header.storageFree': '{storage} Free',
    'header.freeForPath': '{label} for {path}',

    'terminate.reminderTitle': 'Done processing.',
    'terminate.reminderText': 'Before playing, terminate Game Compressor to avoid minor game performance effects.',
    'terminate.confirm': 'Terminate Game Compressor now?\n\nRecommended before playing games because leaving it running can have minor effects on game performance.',
    'terminate.terminating': 'Terminating Game Compressor',
    'terminate.terminated': 'Game Compressor terminated',
    'terminate.exitMessage': 'Exit this window and enjoy playing your games.',

    'history.title': 'History',
    'history.none': 'No operations',
    'history.outputPath': 'output: {path}',
    'history.saved': 'saved {size}',
    'history.scanPercent': 'scan: {percent}%',
    'history.scanSize': 'scan: {size}',
    'history.averageSpeed': 'avg {speed}',

    'storage.internalSsd': 'Internal SSD',
    'storage.externalSsd': 'External SSD',
    'storage.m2Ssd': 'M.2 SSD',
    'storage.externalStorage': 'External Storage',
    'storage.other': 'Other storage',
    'storage.generic': 'storage',
    'storage.usb': 'USB',
    'storage.usbNumber': 'USB {number}',
    'storage.freeOf': '{free} free of {total}',
    'storage.noTarget': 'No available storage target',
    'storage.noOtherUsb': 'No other USB with 10 MB free',
    'storage.connectOtherUsb': 'Connect another USB target with at least 10 MB free',
    'storage.noExternal': 'No external storage with 10 MB free',
    'storage.connectExternal': 'Connect external storage with at least 10 MB free',
    'storage.chooseInternalOrUsb': 'Choose Internal SSD or another USB target',
    'storage.imageTarget': 'Image target: {target}',
    'storage.compressionTarget': 'Compression target: {target}',
    'storage.moveTitle': 'Move {game} to...',
    'storage.copyTitle': 'Copy {game} to...',

    'status.canceling': 'Canceling',
    'status.resolving': 'Resolving',
    'status.measuring': 'Measuring',
    'status.closing': 'Closing',
    'status.hidingOtherInstances': 'Hiding other instances',
    'status.restoringOtherInstances': 'Restoring other instances',
    'status.mounting': 'Mounting',
    'status.deleting': 'Deleting',
    'status.aprIndexed': 'APR Indexed',
    'status.aprPinned': 'APR v{version}',
    'status.buildingAprIndex': 'Building APR Index',
    'status.amprUpdated': 'APR-EMU Updated',
    'status.rebuildingAmpr': 'Rebuilding APR-EMU',
    'status.updatingAmpr': 'Updating APR-EMU',
    'status.testingReadSpeed': 'Testing Read Speed',
    'status.updatingShadowMount': 'Updating ShadowMount',
    'status.publishing': 'Publishing',
    'status.inspecting': 'Inspecting',
    'status.finalizing': 'Finalizing',
    'status.restoring': 'Restoring',
    'status.transferring': 'Transferring',
    'status.copying': 'Copying',
    'status.extracting': 'Extracting',
    'status.repacking': 'Repacking',
    'status.makingImage': 'Making Image',
    'status.unpacking': 'Unpacking',
    'status.validating': 'Validating',
    'status.scanning': 'Scanning',
    'status.compressing': 'Compressing',
    'status.running': 'Running',
    'progress.eta': 'ETA:',
    'status.aprUpdate': 'APR update',
    'status.aprUpdateNeeded': 'APR update needed',
    'status.validated': 'validated',
    'status.changed': 'changed',
    'status.notValidated': 'not validated',
    'status.folder': 'folder',
    'status.image': 'image',
    'status.compressed': 'compressed',
    'status.unavailable': 'unavailable',
    'status.notMounted': 'not mounted',
    'status.aprOriginal': 'APR original',
    'status.aprCustom': 'APR custom',
    'status.aprVersion': 'APR v{version}',
    'status.currentOutput': 'current output',
    'status.originalSource': 'original source',
    'status.locations.one': '{count} location',
    'status.locations.other': '{count} locations',
    'status.savedSize': 'saved {size}',
    'status.sizeQueued': 'size queued',
    'status.scanningSize': 'scanning size',
    'status.measuredFolderSize': 'Measured folder size',
    'status.estimatedFolderSize': 'Estimated folder size, checking for updates',
    'status.cachedFolderSize': 'Cached estimated folder size for display only',
    'status.folderSize': 'Folder size',
    'status.badBlocks.one': '{count} bad block found',
    'status.badBlocks.other': '{count} bad blocks found',
    'status.repairedBlocks.one': '{count} block repaired',
    'status.repairedBlocks.other': '{count} blocks repaired',
    'status.openBadBlockLog': 'Open bad block log',

    'operation.action.updateAmpr': 'Update APR-EMU',
    'operation.action.validateRepair': 'Validate and Repair',
    'operation.action.revalidateRepair': 'Revalidate and Repair',
    'operation.action.compress': 'Compress',
    'operation.action.makeImage': 'Make Image',
    'operation.action.chooseAmpr': 'Manage APR-EMU',
    'operation.action.restoreAmpr': 'Restore Original APR-EMU',
    'operation.action.buildAprIndex': 'Build AMPR Index',
    'operation.action.validateOnly': 'Validate Only',
    'operation.action.uncompress': 'Uncompress',
    'operation.action.setReadOnly': 'Set Read Only',
    'operation.action.extractFolder': 'Extract to Folder',
    'operation.action.copyTo': 'Copy to...',
    'operation.action.moveTo': 'Move to...',
    'operation.action.deleteGameData': 'Delete Game Data',
    'operation.pending.compress': 'Pending Compress',
    'operation.pending.makeImage': 'Pending Make Image',
    'operation.pending.validate': 'Pending Validate',
    'operation.pending.mount': 'Pending Mount',
    'operation.pending.setReadOnly': 'Pending Set Read Only',
    'operation.pending.unpack': 'Pending Unpack',
    'operation.pending.extract': 'Pending Extract',
    'operation.pending.moveExternal': 'Pending Move to External Storage',
    'operation.pending.moveInternal': 'Pending Move to Internal SSD',
    'operation.pending.copyExternal': 'Pending Copy to External Storage',
    'operation.pending.copyInternal': 'Pending Copy to Internal SSD',
    'operation.pending.delete': 'Pending Delete',
    'operation.pending.aprIndex': 'Pending APR Index',
    'operation.pending.amprUpdate': 'Pending APR-EMU Update',
    'operation.pending.readSpeed': 'Pending Read Speed Test',
    'operation.pending.generic': 'Pending Operation',
    'operation.cancelAlreadyRequested': 'Cancel already requested',
    'operation.cancelUnavailable': 'Operation cannot be cancelled now',
    'operation.cancelUnsafe': 'Unsafe compression cannot be cancelled after it starts',
    'operation.mountNow': 'Mount now',
    'operation.testReadSpeed': 'Test read speed',
    'operation.readSpeed': 'Read speed',
    'operation.copyPathFailed': 'Could not copy path',
    'operation.mountFailed': 'Could not mount selected instance',
    'operation.readSpeedStartFailed': 'Could not start read speed test',
    'operation.direction.compressedInPlace': '{source} compressed in place',
    'operation.direction.madeImage': '{source} made into image',
    'operation.direction.unpackedInPlace': '{source} unpacked in place',
    'operation.direction.extractedFolder': '{source} extracted to folder',
    'operation.direction.amprUpdated': '{source} APR-EMU updated',
    'operation.direction.transfer': '{source} -> {target}',

    'result.outputExists': 'output already exists',
    'result.badBlocksNotMounted': "bad blocks found, didn't mount",
    'result.noBadBlocksNotMounted': "no bad blocks found, didn't mount",
    'result.badBlocksFound': 'bad blocks found',
    'result.noBadBlocksFound': 'no bad blocks found',
    'result.unpackedNotMounted': "unpacked, didn't mount",
    'result.mounted': 'mounted',
    'result.notMounted': 'not mounted',
    'result.alreadyReadOnly': 'already read-only',
    'result.setReadOnly': 'set read-only',
    'result.testedReadSpeed': 'tested read speed',
    'result.amprUpdated': 'APR-EMU updated',
    'result.verifiedNotMounted': "verified, didn't mount",

    'failure.compress': 'Compress failed',
    'failure.makeImage': 'Make image failed',
    'failure.unpack': 'Unpack failed',
    'failure.extract': 'Extract failed',
    'failure.validate': 'Validate failed',
    'failure.mount': 'Mount failed',
    'failure.setReadOnly': 'Set read-only failed',
    'failure.aprIndex': 'APR index failed',
    'failure.amprUpdate': 'APR-EMU update failed',
    'failure.readSpeed': 'Read speed failed',
    'failure.operation': 'Operation failed',

    'ampr.versionTitle': 'APR-EMU Version',
    'ampr.versionTitleGame': 'APR-EMU Version - {game}',
    'ampr.availableVersions': 'Available Versions',
    'ampr.uploadCustom': 'Upload Custom File',
    'ampr.loadingVersions': 'Loading APR-EMU versions',
    'ampr.noVersions': 'No APR-EMU versions cached or available',
    'ampr.size': 'Size {size}',
    'ampr.sizeAfterDownload': 'Size after download',
    'ampr.sha': 'SHA {sha}',
    'ampr.shaAfterDownload': 'SHA after download',
    'ampr.storedOnPs5': 'stored on PS5',
    'ampr.downloadsBeforeApplying': 'downloads before applying',
    'ampr.applying': 'Applying...',
    'ampr.preparing': 'Preparing APR-EMU',
    'ampr.selectedInstalled': 'Selected version is already installed.',
    'ampr.selectedCached': 'Selected version is cached and ready to apply.',
    'ampr.selectedDownload': 'Selected version will be downloaded, cached, then applied.',
    'ampr.customPs5Unsupported': 'Upload custom libSceAmpr from your computer. Not supported on PS5 browser.',
    'ampr.hashingUpload': 'Hashing and uploading custom APR-EMU file',
    'ampr.applyingCustom': 'Applying custom APR-EMU file',
    'ampr.applyingCached': 'Applying cached APR-EMU version',
    'ampr.downloadingPreparing': 'Downloading and preparing APR-EMU',
    'ampr.error.manifestFetch': 'APR-EMU manifest fetch failed',
    'ampr.error.manifestMissing': 'APR-EMU manifest did not include libSceAmpr.sprx',
    'ampr.error.download': 'APR-EMU binary download failed',
    'ampr.error.updateCheck': 'APR-EMU update check failed',
    'ampr.error.customEmpty': 'Custom APR-EMU file is empty',
    'ampr.error.customHash': 'Custom APR-EMU SHA-256 could not be computed',
    'ampr.error.latestUnavailable': 'APR-EMU latest cache is unavailable',
    'ampr.error.update': 'APR-EMU update failed',
    'ampr.error.restore': 'Original APR-EMU restore failed',
    'ampr.error.versionList': 'APR-EMU version list failed',
    'ampr.error.customUpload': 'Custom APR-EMU upload failed',
    'ampr.error.noDownloadUrl': 'Selected APR-EMU version has no download URL',
    'ampr.error.noSha': 'Selected APR-EMU SHA-256 is unavailable',
    'ampr.error.apply': 'APR-EMU version apply failed',
    'ampr.error.pin': 'APR-EMU pin failed',
    'ampr.error.delete': 'APR-EMU delete failed',
    'ampr.manageCustom': 'Manage Custom APR',
    'ampr.manageCustomTitle': 'Manage Custom APR',
    'ampr.manageCustomTitleGame': 'Manage Custom APR - {game}',
    'ampr.customVersions': 'Custom APR-EMU Files',
    'ampr.noCustomVersions': 'No custom APR-EMU files',
    'ampr.customUploadHint': 'Upload a libSceAmpr.sprx file to this PS5.',
    'ampr.pinVersion': 'Pin Version',
    'ampr.unpin': 'Unpin',
    'ampr.pinned': 'Pinned',
    'ampr.pinnedStatus': 'Pinned to {version} — APR update stays hidden while this version is installed.',
    'ampr.pinning': 'Pinning APR-EMU version...',
    'ampr.unpinning': 'Unpinning APR-EMU version...',
    'ampr.deleting': 'Deleting custom APR-EMU file...',
    'ampr.confirmDelete': 'Delete this custom APR-EMU file?',

    'compress.dialogTitle': 'Compression Format',
    'compress.dialogGame': 'Compress {game}',
    'compress.makeImageGame': 'Make Image {game}',
    'compress.archive': 'Compressed archive (.ffpfsc)',
    'compress.makeImageHint': 'Need a direct .exfat or .ffpfs file?',
    'compress.makeImageInstead': 'Make Image instead',
    'compress.innerImageFormat': 'Inner image format',
    'compress.innerImageFormatTitle': 'Inner Image Format',
    'compress.imageFormatTitle': 'Image Format',
    'compress.pfsExperimental': 'PFS Experimental',
    'compress.pfsExperimentalFile': 'Experimental - pfs_image.dat',
    'compress.inPlace': 'Compress in place',
    'compress.makeImageInPlace': 'Make image in place',
    'compress.writeCurrent': 'Write next to the current game',
    'compress.writeImageCurrent': 'Write image next to the current game',
    'compress.writeInternal': 'Write compressed file to /data/homebrew',
    'compress.writeImageInternal': 'Write image to /data/homebrew',
    'compress.writeUsb': 'Write compressed file to the USB target below',
    'compress.writeImageUsb': 'Write image to the USB target below',
    'compress.makeImageExternal': 'Make image to external storage',
    'compress.compressTo': 'Compress to...',
    'compress.makeImageTo': 'Make image to...',
    'compress.originalHandling': 'Original Handling',
    'compress.keepOriginal': 'Keep original',
    'compress.originalUntouched': 'Original remains untouched',
    'compress.deleteVerified': 'Delete after verified',
    'compress.requiresFullSize': 'Requires full-size free space',
    'compress.destructive': 'Destructive',
    'compress.requiresOneGb': 'Requires 1 GB free space',
    'compress.notForImage': 'Not available for image creation',
    'compress.exfatImage': 'exFAT image',
    'compress.exfatInside': 'exFAT inside .ffpfsc',
    'compress.pfsImageExperimental': 'PFS image experimental',
    'compress.pfsInsideExperimental': 'PFS inside .ffpfsc experimental',
    'compress.createsExfat': 'Creates {titleId}.ffpfsc with internal exFAT',
    'compress.createsPfs': 'Creates {titleId}.ffpfsc with pfs_image.dat',
    'compress.safe.target': 'Requires target free space; deletes after verification',
    'compress.safe.ssd': 'Requires SSD free space; deletes after verification',
    'compress.safe.measuring': 'Measuring required free space',
    'compress.safe.short': 'Requires {required} free; {extra} short',
    'compress.safe.ready': 'Requires {required} free; deletes after verification',
    'compress.destructive.sameStorage': 'Requires same storage',
    'compress.destructive.folderOnly': 'Requires folder source',
    'compress.destructive.base': 'Requires {required} free; deletes while writing',
    'compress.destructive.short': '{base}; {extra} short',

    'uncompress.dialogTitle': 'Uncompress',
    'uncompress.dialogGame': 'Uncompress {game}',
    'uncompress.detectingImage': 'Detecting image type',
    'uncompress.imageOutput': 'Image output',
    'uncompress.appFolder': 'App folder',
    'uncompress.inPlace': 'In place',
    'uncompress.writeCompressed': 'Write next to the compressed image',
    'uncompress.writeInternal': 'Write to /data/homebrew',
    'uncompress.writeUsb': 'Write to the USB target below',
    'uncompress.originalSource': 'Original source',
    'uncompress.keepBoth': 'Keep both',
    'uncompress.keepCompressed': 'Leave the compressed image after output is written',
    'uncompress.deleteAfter': 'Delete after completion',
    'uncompress.removeCompressed': 'Remove the compressed image after output is ready',
    'uncompress.writeAndKeep': 'Write next to the compressed image and keep the compressed source',
    'uncompress.writeAndRemove': 'Write next to the compressed image and remove it after output is ready',
    'uncompress.exfatImage': 'exFAT image',
    'uncompress.pfsImage': 'PFS image',

    'delete.dialogTitle': 'Delete Game Data',
    'delete.dialogGame': 'Delete {game}',
    'delete.gameData': 'Game data',
    'delete.warning': 'This permanently deletes the selected game data from storage. This cannot be undone.',
    'delete.warningSource': 'This permanently deletes the selected {source} from storage. This cannot be undone.',
    'delete.source.compressed': 'compressed image',
    'delete.source.image': 'image file',
    'delete.source.folder': 'game folder',
    'delete.source.gameData': 'game data',

    'error.scanGames': 'Could not scan games',
    'error.spaceCheckRetry': 'Could not check free storage for this target.\n\nYou can continue, but compression may fail if storage is actually low. The original will be kept until the new file is complete and verified.',
    'error.retryTargetMissing': 'Could not find the original target storage for this retry.',
    'error.lowSpace': 'Not enough free storage.\n\nFree {extra} more on the target storage.',
    'error.lowSpaceDestructive': 'Not enough free storage for destructive compression.\n\nDestructive deletes the source while writing and cannot be cancelled after it starts. It requires {required} free. Free {extra} more on the target storage.',
    'error.lowSpaceSafe': 'Not enough free storage for safe {operation}.\n\nSafe keeps the original until the new file is complete and verified. It requires {required} free. Free {extra} more on the target storage.',
    'error.imageCreation': 'image creation',
    'error.compression': 'compression',
    'error.keepOriginalNeeds': 'Free {extra} more to keep the original untouched until the new file is complete.',
    'error.saferPathConfirm': 'Not enough free storage for the safer path.\n\nDelete while processing can damage the original source if the operation stops.\n\n{detail}',
    'error.lowSpaceDetail': 'Not enough free storage.\n\n{detail}'
  };

  function normalizeLocale(locale) {
    return String(locale || DEFAULT_LOCALE).trim().replace(/_/g, '-').toLowerCase() || DEFAULT_LOCALE;
  }

  function localeCandidates(locale) {
    const normalized = normalizeLocale(locale);
    const language = normalized.split('-')[0];
    return normalized === language ? [normalized] : [normalized, language];
  }

  function catalogFor(locale) {
    const candidates = localeCandidates(locale);
    for (let i = 0; i < candidates.length; i++) {
      if (catalogs[candidates[i]]) return catalogs[candidates[i]];
    }
    return catalogs[DEFAULT_LOCALE];
  }

  function pluralCategory(locale, count) {
    const language = normalizeLocale(locale).split('-')[0];
    const integer = Math.floor(Math.abs(Number(count)));
    if (language === 'ar') {
      if (integer === 0) return 'zero';
      if (integer === 1) return 'one';
      if (integer === 2) return 'two';
      const mod100 = integer % 100;
      if (mod100 >= 3 && mod100 <= 10) return 'few';
      if (mod100 >= 11 && mod100 <= 99) return 'many';
      return 'other';
    }
    if (language === 'fr') return integer === 0 || integer === 1 ? 'one' : 'other';
    return integer === 1 ? 'one' : 'other';
  }

  function catalogValue(catalog, key, category) {
    if (category) {
      const exactVariant = key + '.' + category;
      if (Object.prototype.hasOwnProperty.call(catalog, exactVariant)) return catalog[exactVariant];
      if (category !== 'one') {
        const otherVariant = key + '.other';
        if (Object.prototype.hasOwnProperty.call(catalog, otherVariant)) return catalog[otherVariant];
      }
    }
    if (Object.prototype.hasOwnProperty.call(catalog, key)) return catalog[key];
    return undefined;
  }

  function valueFor(key, params) {
    const count = params && Number(params.count);
    const category = Number.isFinite(count) ? pluralCategory(currentLocale, count) : '';
    const selected = catalogFor(currentLocale) || {};
    const fallback = catalogs[DEFAULT_LOCALE] || {};
    const localized = catalogValue(selected, key, category);
    if (typeof localized !== 'undefined') return localized;
    const fallbackCategory = Number.isFinite(count) ? (count === 1 ? 'one' : 'other') : '';
    const fallbackValue = catalogValue(fallback, key, fallbackCategory);
    if (typeof fallbackValue !== 'undefined') return fallbackValue;
    return key;
  }

  function t(key, params) {
    const template = String(valueFor(String(key || ''), params));
    if (!params) return template;
    return template.replace(/\{([A-Za-z0-9_]+)\}/g, function (match, name) {
      return Object.prototype.hasOwnProperty.call(params, name)
        ? String(params[name])
        : match;
    });
  }

  function paramsForElement(element) {
    const raw = element.getAttribute('data-i18n-params');
    if (!raw) return undefined;
    try {
      return JSON.parse(raw);
    } catch (err) {
      return undefined;
    }
  }

  function applyElement(element) {
    const params = paramsForElement(element);
    const textKey = element.getAttribute('data-i18n');
    const titleKey = element.getAttribute('data-i18n-title');
    const ariaKey = element.getAttribute('data-i18n-aria-label');
    const placeholderKey = element.getAttribute('data-i18n-placeholder');
    if (textKey) element.textContent = t(textKey, params);
    if (titleKey) element.setAttribute('title', t(titleKey, params));
    if (ariaKey) element.setAttribute('aria-label', t(ariaKey, params));
    if (placeholderKey) element.setAttribute('placeholder', t(placeholderKey, params));
  }

  function applyToDom(root) {
    const scope = root || document;
    if (scope.nodeType === 1 && scope.matches && scope.matches(
      '[data-i18n], [data-i18n-title], [data-i18n-aria-label], [data-i18n-placeholder]')) {
      applyElement(scope);
    }
    if (!scope.querySelectorAll) return;
    scope.querySelectorAll(
      '[data-i18n], [data-i18n-title], [data-i18n-aria-label], [data-i18n-placeholder]'
    ).forEach(applyElement);
  }

  function metaFor(locale) {
    const candidates = localeCandidates(locale);
    for (let i = 0; i < candidates.length; i++) {
      if (localeMeta[candidates[i]]) return localeMeta[candidates[i]];
    }
    return localeMeta[DEFAULT_LOCALE];
  }

  function applyDocumentLocale() {
    if (typeof document === 'undefined') return;
    const meta = metaFor(currentLocale) || {};
    document.documentElement.lang = currentLocale;
    document.documentElement.dir = meta.direction === 'rtl' ? 'rtl' : 'ltr';
  }

  function registerLocale(locale, catalog, options) {
    const normalized = normalizeLocale(locale);
    const base = catalogs[normalized] || {};
    catalogs[normalized] = Object.assign(base, catalog || {});
    localeMeta[normalized] = Object.assign(localeMeta[normalized] || {}, options || {});
    return normalized;
  }

  function setLocale(locale) {
    const normalized = normalizeLocale(locale);
    const previousLocale = currentLocale;
    currentLocale = normalized;
    applyDocumentLocale();
    if (typeof document !== 'undefined' && document.documentElement) applyToDom(document);
    const detail = { locale: currentLocale, previousLocale: previousLocale };
    listeners.slice().forEach(function (listener) {
      try { listener(detail); } catch (err) { }
    });
    if (typeof global.CustomEvent === 'function' && global.dispatchEvent) {
      global.dispatchEvent(new CustomEvent('gamecompressor:localechange', { detail: detail }));
    }
    return currentLocale;
  }

  function onLocaleChange(listener) {
    if (typeof listener !== 'function') return function () { };
    listeners.push(listener);
    return function () {
      const index = listeners.indexOf(listener);
      if (index >= 0) listeners.splice(index, 1);
    };
  }

  registerLocale(DEFAULT_LOCALE, english, { direction: 'ltr', label: 'English' });

  const api = {
    DEFAULT_LOCALE: DEFAULT_LOCALE,
    applyToDom: applyToDom,
    getCatalog: function (locale) { return catalogFor(locale); },
    getLocale: function () { return currentLocale; },
    getLocales: function () { return Object.keys(catalogs); },
    getLocaleMeta: function (locale) { return Object.assign({}, metaFor(locale) || {}); },
    onLocaleChange: onLocaleChange,
    registerLocale: registerLocale,
    setLocale: setLocale,
    t: t
  };

  global.GameCompressorI18n = api;
  global.registerGameCompressorLocale = registerLocale;
  global.setGameCompressorLocale = setLocale;
  global.gameCompressorT = t;
  applyDocumentLocale();
})(window);
