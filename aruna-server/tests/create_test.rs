pub mod common;

#[cfg(test)]
mod create_tests {
    use crate::common::{init_test, ADMIN_TOKEN};
    use aruna_rust_api::v3::aruna::api::v3::{
        CreateGroupRequest, CreateProjectRequest, CreateRealmRequest, CreateResourceRequest, Realm,
    };
    use aruna_server::models::requests::{
        BatchResource, CreateLicenseRequest, CreateLicenseResponse, CreateProjectResponse,
        CreateResourceBatchRequest, CreateResourceBatchResponse, UpdateResourceNameRequest,
        UpdateResourceNameResponse,
    };
    use ulid::Ulid;
    pub const OFFSET: u16 = 0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_realm() {
        let mut clients = init_test(OFFSET).await;

        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };

        let response = clients
            .realm_client
            .create_realm(request.clone())
            .await
            .unwrap()
            .into_inner();

        let realm = response.realm.unwrap();

        assert_eq!(&realm.name, &request.name);
        assert_eq!(&realm.tag, &request.tag);
        assert_eq!(&realm.description, &request.description);

        let request = CreateRealmRequest {
            // Same tag
            tag: "test".to_string(),
            name: "SecondTestRealm".to_string(),
            description: String::new(),
        };

        // Tags must be unique
        assert!(clients
            .realm_client
            .create_realm(request.clone())
            .await
            .is_err())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_group() {
        let mut clients = init_test(OFFSET).await;

        let request = CreateGroupRequest {
            name: "TestGroup".to_string(),
            description: String::new(),
        };

        let response = clients
            .group_client
            .create_group(request.clone())
            .await
            .unwrap()
            .into_inner();

        let group = response.group.unwrap();

        assert_eq!(&group.name, &request.name);
        assert_eq!(&group.description, &request.description);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_license() {
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let realm_id = Ulid::from_string(&response.realm.unwrap().id).unwrap();
        let group_id = Ulid::from_string(&response.admin_group_id).unwrap();

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/license", clients.rest_endpoint);

        let request = CreateLicenseRequest {
            name: "CC0".to_string(),
            description: "CC0 enables scientists, educators, artists and other creators and owners of copyright- or database-protected content to waive those interests in their works and thereby place them as completely as possible in the public domain, so that others may freely build upon, enhance and reuse the works for any purposes without restriction under copyright or database law.

In contrast to CC’s licenses that allow copyright holders to choose from a range of permissions while retaining their copyright, CC0 empowers yet another choice altogether – the choice to opt out of copyright and database protection, and the exclusive rights automatically granted to creators – the “no rights reserved” alternative to our licenses.".to_string(),
            license_terms: "Creative Commons Legal Code

CC0 1.0 Universal

    CREATIVE COMMONS CORPORATION IS NOT A LAW FIRM AND DOES NOT PROVIDE
    LEGAL SERVICES. DISTRIBUTION OF THIS DOCUMENT DOES NOT CREATE AN
    ATTORNEY-CLIENT RELATIONSHIP. CREATIVE COMMONS PROVIDES THIS
    INFORMATION ON AN \"AS-IS\" BASIS. CREATIVE COMMONS MAKES NO WARRANTIES
    REGARDING THE USE OF THIS DOCUMENT OR THE INFORMATION OR WORKS
    PROVIDED HEREUNDER, AND DISCLAIMS LIABILITY FOR DAMAGES RESULTING FROM
    THE USE OF THIS DOCUMENT OR THE INFORMATION OR WORKS PROVIDED
    HEREUNDER.

Statement of Purpose

The laws of most jurisdictions throughout the world automatically confer
exclusive Copyright and Related Rights (defined below) upon the creator
and subsequent owner(s) (each and all, an \"owner\") of an original work of
authorship and/or a database (each, a \"Work\").

Certain owners wish to permanently relinquish those rights to a Work for
the purpose of contributing to a commons of creative, cultural and
scientific works (\"Commons\") that the public can reliably and without fear
of later claims of infringement build upon, modify, incorporate in other
works, reuse and redistribute as freely as possible in any form whatsoever
and for any purposes, including without limitation commercial purposes.
These owners may contribute to the Commons to promote the ideal of a free
culture and the further production of creative, cultural and scientific
works, or to gain reputation or greater distribution for their Work in
part through the use and efforts of others.

For these and/or other purposes and motivations, and without any
expectation of additional consideration or compensation, the person
associating CC0 with a Work (the \"Affirmer\"), to the extent that he or she
is an owner of Copyright and Related Rights in the Work, voluntarily
elects to apply CC0 to the Work and publicly distribute the Work under its
terms, with knowledge of his or her Copyright and Related Rights in the
Work and the meaning and intended legal effect of CC0 on those rights.

1. Copyright and Related Rights. A Work made available under CC0 may be
protected by copyright and related or neighboring rights (\"Copyright and
Related Rights\"). Copyright and Related Rights include, but are not
limited to, the following:

  i. the right to reproduce, adapt, distribute, perform, display,
     communicate, and translate a Work;
 ii. moral rights retained by the original author(s) and/or performer(s);
iii. publicity and privacy rights pertaining to a person's image or
     likeness depicted in a Work;
 iv. rights protecting against unfair competition in regards to a Work,
     subject to the limitations in paragraph 4(a), below;
  v. rights protecting the extraction, dissemination, use and reuse of data
     in a Work;
 vi. database rights (such as those arising under Directive 96/9/EC of the
     European Parliament and of the Council of 11 March 1996 on the legal
     protection of databases, and under any national implementation
     thereof, including any amended or successor version of such
     directive); and
vii. other similar, equivalent or corresponding rights throughout the
     world based on applicable law or treaty, and any national
     implementations thereof.

2. Waiver. To the greatest extent permitted by, but not in contravention
of, applicable law, Affirmer hereby overtly, fully, permanently,
irrevocably and unconditionally waives, abandons, and surrenders all of
Affirmer's Copyright and Related Rights and associated claims and causes
of action, whether now known or unknown (including existing as well as
future claims and causes of action), in the Work (i) in all territories
worldwide, (ii) for the maximum duration provided by applicable law or
treaty (including future time extensions), (iii) in any current or future
medium and for any number of copies, and (iv) for any purpose whatsoever,
including without limitation commercial, advertising or promotional
purposes (the \"Waiver\"). Affirmer makes the Waiver for the benefit of each
member of the public at large and to the detriment of Affirmer's heirs and
successors, fully intending that such Waiver shall not be subject to
revocation, rescission, cancellation, termination, or any other legal or
equitable action to disrupt the quiet enjoyment of the Work by the public
as contemplated by Affirmer's express Statement of Purpose.

3. Public License Fallback. Should any part of the Waiver for any reason
be judged legally invalid or ineffective under applicable law, then the
Waiver shall be preserved to the maximum extent permitted taking into
account Affirmer's express Statement of Purpose. In addition, to the
extent the Waiver is so judged Affirmer hereby grants to each affected
person a royalty-free, non transferable, non sublicensable, non exclusive,
irrevocable and unconditional license to exercise Affirmer's Copyright and
Related Rights in the Work (i) in all territories worldwide, (ii) for the
maximum duration provided by applicable law or treaty (including future
time extensions), (iii) in any current or future medium and for any number
of copies, and (iv) for any purpose whatsoever, including without
limitation commercial, advertising or promotional purposes (the
\"License\"). The License shall be deemed effective as of the date CC0 was
applied by Affirmer to the Work. Should any part of the License for any
reason be judged legally invalid or ineffective under applicable law, such
partial invalidity or ineffectiveness shall not invalidate the remainder
of the License, and in such case Affirmer hereby affirms that he or she
will not (i) exercise any of his or her remaining Copyright and Related
Rights in the Work or (ii) assert any associated claims and causes of
action with respect to the Work, in either case contrary to Affirmer's
express Statement of Purpose.

4. Limitations and Disclaimers.

 a. No trademark or patent rights held by Affirmer are waived, abandoned,
    surrendered, licensed or otherwise affected by this document.
 b. Affirmer offers the Work as-is and makes no representations or
    warranties of any kind concerning the Work, express, implied,
    statutory or otherwise, including without limitation warranties of
    title, merchantability, fitness for a particular purpose, non
    infringement, or the absence of latent or other defects, accuracy, or
    the present or absence of errors, whether or not discoverable, all to
    the greatest extent permissible under applicable law.
 c. Affirmer disclaims responsibility for clearing rights of other persons
    that may apply to the Work or any use thereof, including without
    limitation any person's Copyright and Related Rights in the Work.
    Further, Affirmer disclaims responsibility for obtaining any necessary
    consents, permissions or other rights required for any use of the
    Work.
 d. Affirmer understands and acknowledges that Creative Commons is not a
    party to this document and has no duty or obligation with respect to
    this CC0 or use of the Work.".to_string(),
        };

        let response: CreateLicenseResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // Create project
        let request = aruna_server::models::requests::CreateProjectRequest {
            name: "TestLicenseProject".to_string(),
            group_id,
            realm_id,
            visibility: aruna_server::models::models::VisibilityClass::Private,
            license_id: Some(response.license_id),
            ..Default::default()
        };

        let url = format!("{}/api/v3/resources/projects", clients.rest_endpoint);
        let response: CreateProjectResponse = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        println!("{:?}", response);
        assert_eq!(response.resource.license_id, request.license_id.unwrap());

        let request = aruna_server::models::requests::CreateProjectRequest {
            name: "TestInvalidLicenseProject".to_string(),
            group_id,
            realm_id,
            visibility: aruna_server::models::models::VisibilityClass::Private,
            license_id: Some(Ulid::new()),
            ..Default::default()
        };
        assert!(client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json::<CreateProjectResponse>()
            .await
            .is_err())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_project() {
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            title: "This is a Title".to_string(),
            ..Default::default()
        };
        let response = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap();

        println!("{:?}", response);

        assert_eq!(response.name, request.name);
        assert_eq!(response.visibility, 1);
        assert_eq!(response.title, request.title);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_resource_creation() {
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap()
            .id;

        // Create resource
        let request = CreateResourceRequest {
            name: "TestResource".to_string(),
            parent_id,
            visibility: 1,
            variant: 2,
            ..Default::default()
        };
        let resource = clients
            .resource_client
            .create_resource(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap();

        assert_eq!(request.name, resource.name);

        let request = UpdateResourceNameRequest {
            id: Ulid::from_string(&resource.id).unwrap(),
            name: "NewName".to_string(),
            ..Default::default()
        };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/name", clients.rest_endpoint);
        let response: UpdateResourceNameResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(response.resource.name, request.name);
        let new = response.resource.last_modified;
        let old = resource.last_modified.unwrap().into();
        assert!(new > old)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_resource_batch() {
        // Setup
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = Ulid::from_string(
            &clients
                .resource_client
                .create_project(request.clone())
                .await
                .unwrap()
                .into_inner()
                .resource
                .unwrap()
                .id,
        )
        .unwrap();

        // Check if linking works
        let mut resources = Vec::new();
        for i in 0..1000 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("TestObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            } else {
                resources.push(BatchResource {
                    name: format!("TestObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::Idx(i - 1),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            }
        }
        let request = CreateResourceBatchRequest { resources };

        //dbg!(&request);

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);

        let response: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resources.len(), 1000);

        // Check if linking fails when not correctly chaining parents
        let mut resources = Vec::new();
        for i in 0..1000 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("Test2ObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            } else {
                resources.push(BatchResource {
                    name: format!("Test2ObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::Idx(i + 1),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            }
        }
        let request = CreateResourceBatchRequest { resources };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);

        assert!(client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .is_err())
    }
}
