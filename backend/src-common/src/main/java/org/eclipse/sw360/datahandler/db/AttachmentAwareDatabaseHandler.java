/*
 * Copyright Siemens AG, 2017. Part of the SW360 Portal Project.
 *
 * SPDX-License-Identifier: EPL-1.0
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.eclipse.sw360.datahandler.db;

import com.google.common.collect.ImmutableMap;
import org.eclipse.sw360.datahandler.couchdb.DatabaseConnector;
import org.eclipse.sw360.datahandler.thrift.Source;
import org.eclipse.sw360.datahandler.thrift.ThriftClients;
import org.eclipse.sw360.datahandler.thrift.attachments.Attachment;
import org.eclipse.sw360.datahandler.thrift.attachments.AttachmentService;
import org.eclipse.sw360.datahandler.thrift.attachments.CheckStatus;
import org.eclipse.sw360.datahandler.thrift.components.Component;
import org.eclipse.sw360.datahandler.thrift.components.Release;
import org.eclipse.sw360.datahandler.thrift.projects.Project;
import org.ektorp.http.HttpClient;

import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.eclipse.sw360.datahandler.common.CommonUtils.nullToEmptyCollection;
import static org.eclipse.sw360.datahandler.common.CommonUtils.nullToEmptySet;

public abstract class AttachmentAwareDatabaseHandler {

    private AttachmentUsageRepository attachmentUsageRepository;

    protected AttachmentAwareDatabaseHandler(AttachmentUsageRepository repository) {
        attachmentUsageRepository = repository;
    }

    protected AttachmentAwareDatabaseHandler(Supplier<HttpClient> httpClient, String dbName) throws MalformedURLException {
        this(new AttachmentUsageRepository(new DatabaseConnector(httpClient, dbName)));
    }

    protected Source toSource(Release release){
        return Source.releaseId(release.getId());
    }

    protected Source toSource(Component component){
        return Source.releaseId(component.getId());
    }

    protected Source toSource(Project project){
        return Source.releaseId(project.getId());
    }

    public Set<Attachment> getAllAttachmentsToKeep(Source owner, Set<Attachment> originalAttachments, Set<Attachment> changedAttachments) {
        Set<Attachment> attachmentsToKeep = new HashSet<>(nullToEmptySet(changedAttachments));
        Set<String> alreadyPresentIdsInAttachmentsToKeep = nullToEmptyCollection(attachmentsToKeep).stream().map(Attachment::getAttachmentContentId).collect(Collectors.toSet());

        // prevent deletion of already accepted attachments
        Set<Attachment> attachments = nullToEmptySet(originalAttachments);
        Set<Attachment> acceptedAttachmentsNotYetToKeep = attachments.stream().filter(a -> (a.getCheckStatus() == CheckStatus.ACCEPTED && !alreadyPresentIdsInAttachmentsToKeep.contains(a.getAttachmentContentId()))).collect(Collectors.toSet());
        attachmentsToKeep.addAll(acceptedAttachmentsNotYetToKeep);

        // prevent deletion of used attachments
        Set<String> attachmentContentIds = attachments.stream().map(Attachment::getAttachmentContentId).collect(Collectors.toSet());
        String ownerId = owner.getFieldValue().toString();
        ImmutableMap<String, Set<String>> usageSearchParameter = ImmutableMap.of(ownerId, attachmentContentIds);
        Map<Map<String, String>, Integer> attachmentUsageCount = attachmentUsageRepository.getAttachmentUsageCount(usageSearchParameter, null);
        Set<Attachment> usedAttachments = attachments.stream()
                .filter(attachment -> attachmentUsageCount.getOrDefault(ImmutableMap.of(ownerId, attachment.getAttachmentContentId()), 0) > 0)
                .collect(Collectors.toSet());
        attachmentsToKeep.addAll(usedAttachments);

        return attachmentsToKeep;
    }

}
